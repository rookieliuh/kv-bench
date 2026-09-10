/*
 * kv-bench URMA 管理层实现（对齐 yuanrong urma_manager.cpp 精简）。
 */
#include "urma_manager.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../clock.h"
#include "urma_opcode.h"
#include "urma_ubagg.h"

/* 进程级 URMA 库引用计数：同一进程内多个 UrmaManager 实例共享一次
 * urma_init/urma_uninit，解决 bidirectional 双线程冲突的问题 */
static std::atomic<int> g_urma_refcount{0};

namespace kv_bench {

namespace {

constexpr int kMaxPollCr = 32;
constexpr uint64_t kPollSleepNs =
    1000; /* 1us，对齐参考 UrmaManager::PollJfcWait 退避 */

uint64_t NowNs() { return kv_now_ns(); }

/* 本线程 CPU 时间：与 NowNs() 同区间取差用于区分"墙钟等待"与"CPU 消耗"。
 * 注意：event 模式下 poll 线程大部分墙钟时间在 urma_wait_jfc 里睡眠等 CQE，
 * 墙钟-CPU ≈ CQE 到达间隔（正常），只有 CPU 时间本身异常大才是忙转。 */
uint64_t NowCpuNs() {
  struct timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

void SleepNs(uint64_t ns) {
  struct timespec ts = {.tv_sec = 0, .tv_nsec = (long)ns};
  nanosleep(&ts, nullptr);
}

/* 0=RM 1=RC 2=UM 3=RS（对齐 SDK 示例 args_to_trans_mode） */
urma_transport_mode_t ResolveTransMode(uint32_t mode) {
  switch (mode) {
  case 1:
  case 3:
    return URMA_TM_RC;
  case 2:
    return URMA_TM_UM;
  default:
    return URMA_TM_RM;
  }
}

/* 控制面交换的 POD（替代 protobuf） */
struct WireInfo {
  urma_eid_t eid;
  uint32_t uasid;
  uint64_t seg_va;
  uint64_t seg_len;
  uint32_t seg_flag;
  uint32_t seg_token_id;
  urma_jetty_id_t jetty_id;
  uint32_t threads;
  uint32_t op_code;
  uint32_t value_size;
  uint32_t dst_chip;
  uint32_t reserved[3];
} __attribute__((packed));

int SockSyncData(int sockfd, int size, char *localData, char *remoteData) {
  int rc = (int)write(sockfd, localData, (size_t)size);
  if (rc < size) {
    fprintf(stderr, "Failed writing data during exchange, errno: %s.\n",
            strerror(errno));
    return -1;
  }
  int total = 0;
  while (total < size) {
    int n = (int)read(sockfd, remoteData + total, (size_t)(size - total));
    if (n > 0) {
      total += n;
    } else if (n == 0) {
      fprintf(stderr, "Failed reading data during exchange: peer closed\n");
      return -1;
    } else if (errno != EINTR) {
      fprintf(stderr, "Failed reading data during exchange, errno: %s.\n",
              strerror(errno));
      return -1;
    }
  }
  return 0;
}

/* ---------------- PostJettyRw（对齐 yuanrong UrmaManager::PostJettyRw，WR
 * 构造） ---------------- */

// Build a BONDP work request. For WRITE, src SGEs describe local memory and dst
// SGEs describe the remote (imported) memory; for READ (URMA_OPC_READ) the
// caller passes src=dst-direction swapped SGEs (matching YuanRong PostJettyRw).
static urma_jfs_wr_t *
BuildBondpWr(bondp_jfs_wr_t *bondpWr, urma_target_jetty_t *targetJetty,
             urma_sge_t *srcSges, uint32_t numSges, urma_sge_t *dstSges,
             uint64_t userContext, uint32_t useDriverExt, uint32_t srcChipId,
             uint32_t dstChipId, uint32_t fence, urma_opcode_t opcode) {
  auto *wr = &bondpWr->base;
  wr->opcode = opcode;
  wr->flag.bs.complete_enable = 1;
  wr->flag.bs.inline_flag = 0;
  wr->flag.bs.fence = fence != 0;
  wr->flag.bs.has_drv_ext = useDriverExt != 0;
  wr->tjetty = targetJetty;
  wr->user_ctx = userContext;
  wr->rw.src = {.sge = srcSges, .num_sge = numSges};
  wr->rw.dst = {.sge = dstSges, .num_sge = numSges};
  wr->next = nullptr;
  bondpWr->src_chip_id = srcChipId;
  bondpWr->dst_chip_id = dstChipId;
  return wr;
}

// 单 SGE WRITE（数据）：src=本地、dst=远端
static urma_status_t
PostJettyRw(urma_jetty_t *jetty, urma_target_jetty_t *targetJetty,
            urma_target_seg_t *localSeg, urma_target_seg_t *remoteSeg,
            uint64_t localAddress, uint64_t remoteAddress, uint32_t length,
            uint64_t userContext, uint32_t useDriverExt, uint32_t srcChipId,
            uint32_t dstChipId, urma_jfs_wr_t **badWr) {
  if (jetty == nullptr || targetJetty == nullptr || localSeg == nullptr ||
      remoteSeg == nullptr || length == 0) {
    return URMA_EINVAL;
  }
  urma_sge_t localSge = {.addr = localAddress,
                         .len = length,
                         .tseg = localSeg,
                         .user_tseg = nullptr};
  urma_sge_t remoteSge = {.addr = remoteAddress,
                          .len = length,
                          .tseg = remoteSeg,
                          .user_tseg = nullptr};
  bondp_jfs_wr_t bondpWr{};
  urma_jfs_wr_t *wr =
      BuildBondpWr(&bondpWr, targetJetty, &localSge, 1, &remoteSge, userContext,
                   useDriverExt, srcChipId, dstChipId, 0, URMA_OPC_WRITE);
  return urma_post_jetty_send_wr(jetty, wr, badWr);
}

// 单 SGE READ（数据）：src=远端（被读）、dst=本地（数据落点），对齐参考
// UrmaRead（src/dst sge 对调）
static urma_status_t
PostJettyRd(urma_jetty_t *jetty, urma_target_jetty_t *targetJetty,
            urma_target_seg_t *localSeg, urma_target_seg_t *remoteSeg,
            uint64_t localAddress, uint64_t remoteAddress, uint32_t length,
            uint64_t userContext, uint32_t useDriverExt, uint32_t srcChipId,
            uint32_t dstChipId, urma_jfs_wr_t **badWr) {
  if (jetty == nullptr || targetJetty == nullptr || localSeg == nullptr ||
      remoteSeg == nullptr || length == 0) {
    return URMA_EINVAL;
  }
  urma_sge_t localSge = {.addr = localAddress,
                         .len = length,
                         .tseg = localSeg,
                         .user_tseg = nullptr};
  urma_sge_t remoteSge = {.addr = remoteAddress,
                          .len = length,
                          .tseg = remoteSeg,
                          .user_tseg = nullptr};
  bondp_jfs_wr_t bondpWr{};
  urma_jfs_wr_t *wr =
      BuildBondpWr(&bondpWr, targetJetty, &remoteSge, 1, &localSge, userContext,
                   useDriverExt, srcChipId, dstChipId, 0, URMA_OPC_READ);
  return urma_post_jetty_send_wr(jetty, wr, badWr);
}

} // namespace

/* ---------------- UrmaConnection ---------------- */

uint64_t UrmaConnection::RemoteSegVa() const {
  return remoteSeg != nullptr ? remoteSeg->Va() : 0;
}

/* ---------------- UrmaManager ---------------- */

UrmaManager::~UrmaManager() { Stop(); }

int UrmaManager::GetEidIndex(urma_device_t *dev) {
  uint32_t eidCnt = 0;
  urma_eid_info_t *eidList = urma_get_eid_list(dev, &eidCnt);
  if (eidList == nullptr || eidCnt == 0) {
    if (eidList != nullptr) {
      urma_free_eid_list(eidList);
    }
    return -1;
  }
  int index = eidList[0].eid_index;
  urma_free_eid_list(eidList);
  return index;
}

bool UrmaManager::InitUrmaLib() {
  if (g_urma_refcount.fetch_add(1) > 0) {
    return true; /* 已有实例初始化过，仅增加引用计数 */
  }
  urma_init_attr_t attr;
  (void)memset(&attr, 0, sizeof(attr));
  attr.uasid = 0;
  if (urma_init(&attr) != URMA_SUCCESS) {
    g_urma_refcount.fetch_sub(1); /* 回滚引用计数 */
    fprintf(stderr, "Failed to urma_init\n");
    return false;
  }
  return true;
}

bool UrmaManager::Init(const std::string &devName, bool cacheable,
                       uint32_t jettyCount, uint32_t threadsMin, bool eventMode,
                       uint32_t transMode, int eidIndex) {
  if (initialized_) {
    return true;
  }
  eventMode_ = eventMode;
  if (!InitUrmaLib()) {
    return false;
  }
  urma_device_t *dev =
      urma_get_device_by_name(const_cast<char *>(devName.c_str()));
  if (dev == nullptr) {
    fprintf(stderr, "urma_get_device_by_name failed: %s\n", devName.c_str());
    return false;
  }
  int eid = eidIndex;
  if (eid < 0) {
    eid = GetEidIndex(dev);
    if (eid < 0) {
      fprintf(stderr, "Failed to get eid index\n");
      return false;
    }
  }
  if (!resource_.Init(dev, (uint32_t)eid, cacheable, jettyCount, threadsMin,
                      eventMode_, transMode)) {
    return false;
  }
  if (eventMode_ && !resource_.RearmJfc()) {
    fprintf(stderr, "Failed to rearm jfc\n");
    return false;
  }
  stop_ = false;
  initialized_ = true;
  /* 轮询线程 Init 即启动（对齐参考 UrmaManager::Init 起 UrmaPollJfc）：
   * 服务器 write 模式无打流线程，也必须推进本地 JFC 让路径建立/应答 */
  return EnsurePollThread();
}

void UrmaManager::Stop() {
  if (initialized_) {
    stop_ = true;
    if (pollStarted_) {
      if (pollThread_.joinable()) {
        pollThread_.join();
      }
      pollStarted_ = false;
    }
    resource_.Clear();
    initialized_ = false;
    /* 所有 urma 资源（本地段等）必须在 urma_uninit 之前释放：
     * localSeg_ 的析构会调 urma_unregister_seg，uninit 之后再调会段错误 */
    localSeg_.reset();
    buf_ = nullptr;
    bufLen_ = 0;
    if (g_urma_refcount.fetch_sub(1) == 1) {
      (void)urma_uninit();
    }
  }
}

bool UrmaManager::RegisterBuffer(void *va, uint64_t len) {
  std::shared_ptr<UrmaLocalSegment> seg;
  if (!resource_.RegisterSegment((uint64_t)va, len, seg)) {
    return false;
  }
  buf_ = va;
  bufLen_ = len;
  localSeg_ = seg;
  return true;
}

urma_target_seg_t *UrmaManager::LocalSeg() const {
  return localSeg_ != nullptr ? localSeg_->Raw() : nullptr;
}

/* ---------------- 事件槽 ---------------- */

bool UrmaManager::RegisterWorker(uint32_t &workerId) {
  bool first = false;
  {
    std::lock_guard<std::mutex> lock(workerRegisterMutex_);
    const uint32_t count = workerCount_.load(std::memory_order_relaxed);
    if (count >= kMaxRegisteredWorkers) {
      fprintf(stderr, "Too many URMA workers (max=%u)\n",
              kMaxRegisteredWorkers);
      return false;
    }
    workerSlots_[count] = std::make_unique<EventSlots>();
    workerId = count;
    first = (count == 0);
    workerCount_.store(count + 1, std::memory_order_release);
  }
  if (first) {
    EnsurePollThread();
  }
  return true;
}

void UrmaManager::UnregisterWorker(uint32_t workerId) {
  /* 事件槽保留到 Stop（打流线程全部结束后统一清理）；此处仅占位 */
  (void)workerId;
}

uint32_t UrmaManager::WorkerCount() const {
  return workerCount_.load(std::memory_order_acquire);
}

UrmaManager::EventSlots *UrmaManager::GetEventSlots(uint32_t workerId) const {
  if (workerId >= workerCount_.load(std::memory_order_acquire)) {
    return nullptr;
  }
  return workerSlots_[workerId].get();
}

uint64_t UrmaManager::PostEvent(uint32_t workerId, uint64_t &eventToken) {
  EventSlots *slots = GetEventSlots(workerId);
  if (slots == nullptr || !slots->Acquire(eventToken)) {
    eventToken = 0;
    return 0;
  }
  return ((uint64_t)workerId << kRidShift) | eventToken;
}

bool UrmaManager::WaitEvent(uint32_t workerId, uint64_t eventToken,
                            int timeoutMs) {
  EventSlots *slots = GetEventSlots(workerId);
  if (slots == nullptr) {
    return false;
  }
  uint64_t deadline = NowNs() + (uint64_t)timeoutMs * 1000000ULL;
  for (;;) {
    int result = slots->Probe(eventToken);
    if (result != 0) {
      return result == 1;
    }
    if (NowNs() >= deadline) {
      break;
    }
    SleepNs(kPollSleepNs);
  }
  int result = slots->Probe(eventToken); /* deadline 边界再检查一次 */
  if (result != 0) {
    return result == 1;
  }
  (void)slots->Abort(eventToken);
  return false;
}

int UrmaManager::ProbeEvent(uint32_t workerId, uint64_t eventToken) {
  EventSlots *slots = GetEventSlots(workerId);
  if (slots == nullptr) {
    return 0;
  }
  return slots->Probe(eventToken);
}

void UrmaManager::AbortEvent(uint32_t workerId, uint64_t eventToken) {
  EventSlots *slots = GetEventSlots(workerId);
  if (slots == nullptr) {
    return;
  }
  (void)slots->Abort(eventToken);
}

void UrmaManager::CompleteEvent(uint32_t workerId, uint64_t userCtx, bool ok) {
  uint64_t eventToken = userCtx & kRidMask;
  if ((userCtx >> kRidShift) != workerId) {
    return;
  }
  EventSlots *slots = GetEventSlots(workerId);
  if (slots == nullptr) {
    return;
  }
  slots->Complete(eventToken, ok);
}

/* ---------------- 轮询线程 ---------------- */

bool UrmaManager::EnsurePollThread() {
  if (pollStarted_) {
    return true;
  }
  try {
    pollThread_ = std::thread(&UrmaManager::PollThreadMain, this);
    pollStarted_ = true;
    return true;
  } catch (...) {
    fprintf(stderr, "Failed to start poll thread\n");
    return false;
  }
}

void UrmaManager::PollThreadMain() {
  /* 轮询线程绑核（与打流 worker 区分开），并打印所在 CPU */
  if (pollCpu_ >= 0) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(pollCpu_, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
      fprintf(stderr, "Failed to pin poll thread to cpu %d: %s\n", pollCpu_,
              strerror(errno));
    } else {
      printf("[poll] poll thread pinned to cpu %d\n", pollCpu_);
    }
  }
  urma_cr_t crs[kMaxPollCr];
  uint64_t lastPollNs_ = 0;
  uint64_t maxPollNs = 0;
  uint64_t lastOnlyPollNs_ = 0;
  uint64_t maxOnlyPollNs = 0;
  while (!stop_) {
    int cnt;
    if (eventMode_) {
      urma_jfc_t *evJfc = nullptr;
      cnt = urma_wait_jfc(resource_.Jfce(), 1, 100, &evJfc);
      if (cnt <= 0) {
        continue;
      }
      if (resource_.Jfc() != evJfc) {
        continue;
      }
      cnt = urma_poll_jfc(resource_.Jfc(), kMaxPollCr, crs);
      if (cnt > 0) {
        uint32_t ack = (uint32_t)cnt;
        urma_ack_jfc(&evJfc, &ack, 1);
        (void)urma_rearm_jfc(resource_.Jfc(), false);
      }
    } else {
      cnt = urma_poll_jfc(resource_.Jfc(), kMaxPollCr, crs);
    }
    if (lastOnlyPollNs_ != 0) {
      uint64_t curOnlyPollNs = NowNs() - lastOnlyPollNs_;
      if (curOnlyPollNs > maxOnlyPollNs) {
        maxOnlyPollNs = curOnlyPollNs;
      }
    }
    if (cnt > 0) {
      for (int i = 0; i < cnt; i++) {
        uint64_t userCtx = crs[i].user_ctx;
        uint32_t wid = (uint32_t)(userCtx >> kRidShift);
        if (crs[i].status != URMA_CR_SUCCESS) {
          fprintf(stderr, "[poll] CQE failed: status=%d user_ctx=%lu\n",
                  crs[i].status, (unsigned long)userCtx);
        }
        CompleteEvent(wid, userCtx, crs[i].status == URMA_CR_SUCCESS);
      }

      uint64_t now = NowNs();
      uint64_t cpuNow = NowCpuNs();
      if (lastPollNs_ != 0) {
        uint64_t curPollNs = now - lastPollNs_;
        // if (curPollNs > 500000ULL) {
        //   fprintf(stderr, "[poll] poll interval %.3f us, now=%.3f\n",
        //           (double)curPollNs / 1000.0, (double)now / 1000.0);
        // }
        if (curPollNs > maxPollNs) {
          maxPollNs = curPollNs;
        }
      }
      lastPollNs_ = now;
    } else if (cnt < 0) {
      fprintf(stderr, "Failed to poll jfc, ret=%d\n", cnt);
      SleepNs(1000 * kPollSleepNs);
    } else {
      SleepNs(kPollSleepNs);
    }
    lastOnlyPollNs_ = NowNs();
  }

  printf("[poll] poll thread exiting, max poll interval %.3f us "
         "max only poll "
         "interval %.3f us\n",
         (double)maxPollNs / 1000.0, (double)maxOnlyPollNs / 1000.0);
}

/* ---------------- send lane（每轮从池取一条 jetty） ---------------- */

bool UrmaManager::AcquireSendLane(std::shared_ptr<UrmaJetty> &jetty) {
  return resource_.AcquireSendJetty(jetty);
}

void UrmaManager::ReleaseSendLane(const std::shared_ptr<UrmaJetty> &jetty) {
  resource_.ReleaseSendJetty(jetty);
}

/* ---------------- 握手 ---------------- */

bool UrmaManager::Exchange(int sockfd, const HandshakeParams &params,
                           std::shared_ptr<UrmaConnection> &conn,
                           bool preferRtp) {
  if (localSeg_ == nullptr || resource_.Ctx() == nullptr) {
    fprintf(stderr, "Exchange before RegisterBuffer/Init\n");
    return false;
  }
  urma_target_seg_t *local = localSeg_->Raw();

  WireInfo localWire;
  (void)memset(&localWire, 0, sizeof(localWire));
  localWire.eid = resource_.Eid();
  localWire.uasid = resource_.Uasid();
  localWire.seg_va = local->seg.ubva.va;
  localWire.seg_len = local->seg.len;
  localWire.seg_flag = local->seg.attr.value;
  localWire.seg_token_id = local->seg.token_id;
  {
    /* 发布进程级 RECV jetty（独立 JFR）供对端 import（对齐 yuanrong
     * GetOrCreateSharedRecvJetty + 握手发布） */
    std::shared_ptr<UrmaJetty> recvJetty = resource_.RecvJetty();
    if (recvJetty != nullptr) {
      localWire.jetty_id = recvJetty->Raw()->jetty_id;
    }
  }
  localWire.threads = params.threads;
  localWire.op_code = params.opCode;
  localWire.value_size = params.valueSize;
  localWire.dst_chip = params.dstChip;

  WireInfo remoteWire;
  (void)memset(&remoteWire, 0, sizeof(remoteWire));
  if (SockSyncData(sockfd, (int)sizeof(WireInfo), (char *)&localWire,
                   (char *)&remoteWire) != 0) {
    return false;
  }

  printf("exchange: remote seg va=0x%lx len=%lu jetty=%u dst_chip=%u "
         "threads=%u op=%u\n",
         (uint64_t)remoteWire.seg_va, (uint64_t)remoteWire.seg_len,
         remoteWire.jetty_id.id, remoteWire.dst_chip, remoteWire.threads,
         remoteWire.op_code);

  auto newConn = std::make_shared<UrmaConnection>();
  newConn->peer.eid = remoteWire.eid;
  newConn->peer.uasid = remoteWire.uasid;
  newConn->peer.jettyId = remoteWire.jetty_id.id;
  newConn->peer.dstChip = remoteWire.dst_chip;
  newConn->peer.threads = remoteWire.threads;
  newConn->peer.opCode = remoteWire.op_code;
  newConn->peer.valueSize = remoteWire.value_size;
  newConn->peer.seg.ubva.eid = remoteWire.eid;
  newConn->peer.seg.ubva.uasid = remoteWire.uasid;
  newConn->peer.seg.ubva.va = remoteWire.seg_va;
  newConn->peer.seg.len = remoteWire.seg_len;
  newConn->peer.seg.attr.value = remoteWire.seg_flag;
  newConn->peer.seg.token_id = remoteWire.seg_token_id;

  /* import 对端 jetty（legacy handshake，对齐 yuanrong BuildRemoteJetty +
   * ImportTargetJetty）。
   * 默认路径1（bonding/datasystem）: bondp_rjetty + tp_type=CTP +
   * has_drv_ext=1； preferRtp 时直接走路径2（SDK 示例）: 普通 urma_rjetty +
   * tp_type=RTP。 bondp.jetty 传 nullptr（对齐参考
   * ImportRemoteJetty/FinalizeOutboundConnection）。 */
  bool okImport = false;
  if (preferRtp) {
    urma_rjetty_t rjetty;
    (void)memset(&rjetty, 0, sizeof(rjetty));
    rjetty.jetty_id = remoteWire.jetty_id;
    rjetty.trans_mode = ResolveTransMode(params.transMode);
    rjetty.type = URMA_JETTY;
    rjetty.tp_type = URMA_RTP;
    rjetty.flag.bs.order_type = params.transMode == 3 ? 1 : 0;
    rjetty.flag.bs.share_tp = params.transMode == 3 ? 1 : 0;
    okImport = resource_.ImportTargetJetty(&rjetty, newConn->targetJetty);
    if (okImport) {
      printf("import remote jetty ok (RTP path)\n");
    }
  } else {
    bondp_rjetty_t bondp;
    (void)memset(&bondp, 0, sizeof(bondp));
    bondp.base.jetty_id = remoteWire.jetty_id;
    bondp.base.trans_mode = ResolveTransMode(params.transMode);
    bondp.base.type = URMA_JETTY;
    bondp.base.tp_type = URMA_CTP;
    bondp.base.flag.bs.has_drv_ext = 1;
    bondp.jetty = nullptr;
    if (!resource_.ImportTargetJetty(&bondp.base, newConn->targetJetty)) {
      /* 回退：SDK 示例 legacy 路径（RTP） */
      urma_rjetty_t rjetty;
      (void)memset(&rjetty, 0, sizeof(rjetty));
      rjetty.jetty_id = remoteWire.jetty_id;
      rjetty.trans_mode = ResolveTransMode(params.transMode);
      rjetty.type = URMA_JETTY;
      rjetty.tp_type = URMA_RTP;
      rjetty.flag.bs.order_type = params.transMode == 3 ? 1 : 0;
      rjetty.flag.bs.share_tp = params.transMode == 3 ? 1 : 0;
      okImport = resource_.ImportTargetJetty(&rjetty, newConn->targetJetty);
      if (okImport) {
        printf("import remote jetty ok (RTP legacy path)\n");
      }
    } else {
      okImport = true;
      printf("import remote jetty ok (bondp CTP path)\n");
    }
  }
  if (!okImport) {
    fprintf(stderr,
            "import remote jetty failed: id=%u eid=" EID_FMT " uasid=0x%x\n",
            remoteWire.jetty_id.id, EID_ARGS(remoteWire.eid), remoteWire.uasid);
    return false;
  }
  if (!resource_.ImportSegment(newConn->peer.seg, newConn->remoteSeg)) {
    return false;
  }

  /* RC 模式: 绑定所有本地 jetty 到对端 target jetty（对齐 SDK 示例） */
  if (params.transMode == 1 || params.transMode == 3) {
    uint32_t n = resource_.SendJettyCount();
    for (uint32_t i = 0; i < n; i++) {
      std::shared_ptr<UrmaJetty> jetty = resource_.JettyAt(i);
      if (jetty != nullptr &&
          urma_bind_jetty(jetty->Raw(), newConn->targetJetty->Raw()) !=
              URMA_SUCCESS) {
        fprintf(stderr, "Failed to bind jetty to remote target jetty\n");
        return false;
      }
    }
    printf("bind %u jettys to remote jetty\n", n);
  }
  conn = std::move(newConn);
  return true;
}

bool UrmaManager::ExchangeAsClient(int sockfd, const HandshakeParams &params,
                                   std::shared_ptr<UrmaConnection> &conn,
                                   bool preferRtp) {
  return Exchange(sockfd, params, conn, preferRtp);
}

bool UrmaManager::ExchangeAsServer(int sockfd, const HandshakeParams &params,
                                   std::shared_ptr<UrmaConnection> &conn,
                                   bool preferRtp) {
  return Exchange(sockfd, params, conn, preferRtp);
}

/* ---------------- 数据面 ---------------- */

bool UrmaManager::PostWrite(const std::shared_ptr<UrmaJetty> &jetty,
                            UrmaConnection &conn, uint64_t localAddr,
                            uint64_t remoteAddr, uint32_t len, uint32_t srcChip,
                            uint32_t dstChip, uint64_t userCtx) {
  if (jetty == nullptr || localSeg_ == nullptr || conn.targetJetty == nullptr ||
      conn.remoteSeg == nullptr) {
    return false;
  }
  {
    /* 前 4 条 WR 打印 sge/tseg 详情，便于定位 status=LOC_ACCESS_ERR 等失败 */
    static std::atomic<int> diagCnt{0};
    if (diagCnt.fetch_add(1) < 4) {
      urma_seg_t *ls = &localSeg_->Raw()->seg;
      urma_seg_t *rs = &conn.remoteSeg->Raw()->seg;
      fprintf(stderr,
              "[wr] diag: local=0x%lx remote=0x%lx len=%u src=%u dst=%u "
              "local_tseg_va=0x%lx local_len=%llu remote_tseg_va=0x%lx "
              "remote_len=%llu jetty=%u target_jetty=0x%lx\n",
              (unsigned long)localAddr, (unsigned long)remoteAddr, len, srcChip,
              dstChip, (unsigned long)ls->ubva.va, (unsigned long long)ls->len,
              (unsigned long)rs->ubva.va, (unsigned long long)rs->len,
              jetty->Raw()->jetty_id.id,
              (unsigned long)conn.targetJetty->Raw());
    }
  }
  uint32_t drv = (srcChip != kInvalidChip && dstChip != kInvalidChip) ? 1 : 0;
  urma_jfs_wr_t *bad = nullptr;
  urma_status_t rc =
      PostJettyRw(jetty->Raw(), conn.targetJetty->Raw(), localSeg_->Raw(),
                  conn.remoteSeg->Raw(), localAddr, remoteAddr, len, userCtx,
                  drv, srcChip, dstChip, &bad);
  if (rc != URMA_SUCCESS) {
    fprintf(stderr, "[wr] post failed rc=%d\n", rc);
  }
  return rc == URMA_SUCCESS;
}

bool UrmaManager::PostRead(const std::shared_ptr<UrmaJetty> &jetty,
                           UrmaConnection &conn, uint64_t localAddr,
                           uint64_t remoteAddr, uint32_t len, uint32_t srcChip,
                           uint32_t dstChip, uint64_t userCtx) {
  if (jetty == nullptr || localSeg_ == nullptr || conn.targetJetty == nullptr ||
      conn.remoteSeg == nullptr) {
    return false;
  }
  uint32_t drv = (srcChip != kInvalidChip && dstChip != kInvalidChip) ? 1 : 0;
  urma_jfs_wr_t *bad = nullptr;
  urma_status_t rc =
      PostJettyRd(jetty->Raw(), conn.targetJetty->Raw(), localSeg_->Raw(),
                  conn.remoteSeg->Raw(), localAddr, remoteAddr, len, userCtx,
                  drv, srcChip, dstChip, &bad);
  if (rc != URMA_SUCCESS) {
    fprintf(stderr, "[rd] post failed rc=%d\n", rc);
  }
  return rc == URMA_SUCCESS;
}

} // namespace kv_bench

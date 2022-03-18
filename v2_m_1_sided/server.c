#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/time.h>
#include <rdma/rdma_cma.h>
#include <unistd.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define PAGE_SIZE 4096
#define NR_LOOPS 300
const int BUFFER_SIZE = 1024;
const int MESSAGE_SIZE = 1024;
const int MEMORY_BUFFER_SIZE = 10 * 1024 * 1024;
const int NR_MEMORY_PAGES = 10 * 1024 * 1024 / 4096;
const int COMPUTE_BUFFER_SIZE = 1 * 1024 * 1024;
const int NR_COMPUTE_PAGES = 1 * 1024 * 1024 / 4096;

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct message {
  enum {
    MSG_MR,
    MSG_DONE
  } type;

  union {
    struct ibv_mr mr;
  } data;
  
  union {
    struct ibv_mr mr;
  } cache;

  union {
    struct ibv_mr mr;
  } ready;
};

struct ready_info {
  int addr;
  int ready_flag;
};



struct connection {
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *rdma_local_mr;
  struct ibv_mr *rdma_cache_mr;
  struct ibv_mr *ready_mr;


  int connected;
    enum {
    SS_INIT,
    SS_MR_SENT,
    SS_WRITE_READY_SENT,
    SS_WRITE_DATA_SENT,
    SS_READ_READY_SENT,
    SS_READ_DATA_SENT,
    SS_DONE_SENT
  } send_state;

  enum {
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV
  } recv_state;

  char *recv_region;
  char *send_region;
  struct message *send_msg;
  struct message *recv_msg;
  struct ready_info *ready;

  char *local_cache;
  char *rdma_local_region;
  int fd;
  const char *file_name;
  int num_completions;
  int random_access_read[1000];
  int random_access_write[1000];
  struct timeval tv_begin, tv_end;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
#if _USE_IPV6
  struct sockaddr_in6 addr;
#else
  struct sockaddr_in addr;
#endif
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  memset(&addr, 0, sizeof(addr));
#if _USE_IPV6
  addr.sin6_family = AF_INET6;
#else
  addr.sin_family = AF_INET;
#endif

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  
  posix_memalign((void **)&conn->local_cache, sysconf(_SC_PAGESIZE), COMPUTE_BUFFER_SIZE);
  posix_memalign((void **)&conn->rdma_local_region, sysconf(_SC_PAGESIZE), COMPUTE_BUFFER_SIZE);
  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));
  conn->ready = malloc(sizeof(struct ready_info));
  conn->ready->addr = 2;
  conn->ready->ready_flag = 0;
  

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    0));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE));

  TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    COMPUTE_BUFFER_SIZE, 
    (IBV_ACCESS_LOCAL_WRITE |  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)));
  
  TEST_Z(conn->rdma_cache_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->local_cache, 
    COMPUTE_BUFFER_SIZE, 
    (IBV_ACCESS_LOCAL_WRITE |  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)));

  TEST_Z(conn->ready_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->ready, 
    sizeof(struct ready_info), 
    (IBV_ACCESS_LOCAL_WRITE |  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)));
  conn->file_name = basename("test_file");
  conn->fd = open("test_file", O_RDONLY);

  if (conn->fd == -1) {
    fprintf(stderr, "unable to open input file \"%s\"\n", "test_file");
  }

  ssize_t size = 0;
  size = read(conn->fd, conn->local_cache, COMPUTE_BUFFER_SIZE);
  if (size == -1) {
    fprintf(stderr, "read failed\n");
  }
  srand(0);
  for (int i = 0; i <1000; i++){
    conn->random_access_read[i] = rand() % NR_MEMORY_PAGES;
  }
  srand(1);
  for (int i = 0; i <1000; i++){
    conn->random_access_write[i] = rand() % NR_COMPUTE_PAGES;
  } 

}

void send_mr(struct connection *conn)
{
  conn->send_msg->type = MSG_MR;
  memcpy(&conn->send_msg->data.mr, conn->rdma_local_mr, sizeof(struct ibv_mr));
  memcpy(&conn->send_msg->ready.mr, conn->ready_mr, sizeof(struct ibv_mr));
  memcpy(&conn->send_msg->cache.mr, conn->rdma_cache_mr, sizeof(struct ibv_mr));
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_mr->lkey;

  while (!conn->connected);
  conn->send_state = SS_MR_SENT;
  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void on_completion(struct ibv_wc *wc)
{
  if (wc->status != IBV_WC_SUCCESS)
    {printf("stats: %d\n", wc->status);
    die("on_completion: status is not IBV_WC_SUCCESS.");}

  if (wc->opcode & IBV_WC_RECV) {
    printf("receive mr\n");
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
    // post_receives(conn);
    if (conn->send_state == SS_INIT){
      send_mr(conn);
      gettimeofday(&conn->tv_begin, NULL);
      while (conn->num_completions < NR_LOOPS){
        // printf("current ready flag: %d\n", conn->ready->ready_flag);
        conn->ready->addr = conn->random_access_write[conn->num_completions];
        conn->ready->ready_flag = 1;
        conn->num_completions++;
        while(conn->ready->ready_flag != 0){
          // wait until memory finishes
        }
        for (int i = 0; i < PAGE_SIZE; i++){
          conn->local_cache[i] += conn->rdma_local_region[i];
        }
      }
      gettimeofday(&conn->tv_end, NULL);
      printf("overall time is %f\n",  (conn->tv_end.tv_sec - conn->tv_begin.tv_sec) + (conn->tv_end.tv_usec - conn->tv_begin.tv_usec)/1000000.0);
      
      
      
    }
    
  } else if (wc->opcode == IBV_WC_SEND) {
    printf("send completed successfully.\n");
  }
}
void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}
int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  printf("received connection request.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  conn->qp = id->qp;

  conn->connected = 1;
  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  register_memory(conn);
  post_receives(conn);

  build_params(&cm_params);
  TEST_NZ(rdma_accept(id, &cm_params));
  return 0;
}

int on_connection(void *context)
{
  ((struct connection *)context)->connected = 1;
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);
  ibv_dereg_mr(conn->rdma_local_mr);

  free(conn->send_region);
  free(conn->recv_region);
  free(conn->send_msg);
  free(conn->recv_msg);
  free(conn->local_cache);

  free(conn);

  rdma_destroy_id(id);

  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}


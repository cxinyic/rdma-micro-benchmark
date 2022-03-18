#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define PAGE_SIZE 4096
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
  int addr;
  char data[PAGE_SIZE];
};


struct connection {
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
   

  enum {
    SS_INIT,
    SS_WRITE_SENT,
    SS_READ_SENT,
    SS_DONE_SENT
  } send_state;

  enum {
    RS_INIT,
    RS_WRITE_RECV,
    RS_READ_RECV,
    RS_DONE_RECV
  } recv_state;

  char *recv_region;
  char *send_region;
  struct message *send_msg;
  struct message *recv_msg;

  char *buffer_pool;
  int fd;
  const char *file_name;
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
  // posix_memalign((void **)&conn->send_region, sysconf(_SC_PAGESIZE), MEMORY_BUFFER_SIZE);
  // posix_memalign((void **)&conn->recv_region, sysconf(_SC_PAGESIZE), MEMORY_BUFFER_SIZE);
  posix_memalign((void **)&conn->buffer_pool, sysconf(_SC_PAGESIZE), MEMORY_BUFFER_SIZE);
  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));
  //conn->send_msg->data = malloc(PAGE_SIZE);
  //conn->recv_msg->data = malloc(PAGE_SIZE);

  /*TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->send_region,
    MEMORY_BUFFER_SIZE,
    0));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->recv_region,
    MEMORY_BUFFER_SIZE,
    IBV_ACCESS_LOCAL_WRITE));*/

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

  conn->file_name = basename("test_file");
  conn->fd = open("test_file", O_RDONLY);

  if (conn->fd == -1) {
    fprintf(stderr, "unable to open input file \"%s\"\n", "test_file");
  }

  ssize_t size = 0;
  size = read(conn->fd, conn->buffer_pool, MEMORY_BUFFER_SIZE);
  if (size == -1) {
    fprintf(stderr, "read failed\n");
  }

}
void send_write_response(struct connection *conn){
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  // local write
  
  struct message *recv = conn->recv_msg;
  memcpy(conn->buffer_pool + (recv->addr * PAGE_SIZE), conn->buffer_pool, PAGE_SIZE);
  
  // write ack
  snprintf(conn->send_msg->data, PAGE_SIZE, "received write request, ack\n");
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_mr->lkey;

  conn->send_state = SS_WRITE_SENT;
  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_read_response(struct connection *conn, int data){
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  // local read
  struct message *recv = conn->recv_msg;
  memcpy(conn->send_msg->data, conn->buffer_pool + (recv->addr * PAGE_SIZE), PAGE_SIZE);
  // snprintf(conn->send_msg->data, PAGE_SIZE, "the data for read request is %d\n", data);
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_mr->lkey;


  conn->send_state = SS_READ_SENT;
  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void on_completion(struct ibv_wc *wc)
{
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV) {
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

    // printf("received message: %s\n", conn->recv_region);
    post_receives(conn);
    // printf("current send state is %d\n", conn->send_state);
    if (conn->send_state == SS_WRITE_SENT) {
      send_read_response(conn, 100);
      // printf("send read response\n");
    }
    else if  (conn->send_state == SS_READ_SENT || conn->send_state == SS_INIT) {
      send_write_response(conn);
      // printf("send write response\n");
    }
  } else if (wc->opcode == IBV_WC_SEND) {
    // printf("send completed successfully.\n");
  }
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

  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  register_memory(conn);
  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));
  return 0;
}

int on_connection(void *context)
{
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);
  // free(conn->send_msg->data);
  // free(conn->recv_msg->data);
  free(conn->send_msg);
  free(conn->recv_msg);

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


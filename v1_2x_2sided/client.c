#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include<sys/time.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define PAGE_SIZE 4096
#define NR_LOOPS 300
const int BUFFER_SIZE = 1024;
const int TIMEOUT_IN_MS = 500; /* ms */
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
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  char *recv_region;
  char *send_region;
  struct message *send_msg;
  struct message *recv_msg;

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
  
  char *local_cache;
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

static int on_addr_resolved(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static int on_route_resolved(struct rdma_cm_id *id);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;

  if (argc != 3)
    die("usage: client <server-address> <server-port>");

  TEST_NZ(getaddrinfo(argv[1], argv[2], NULL, &addr));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

  freeaddrinfo(addr);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

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
  // conn->send_region = malloc(BUFFER_SIZE);
  // conn->recv_region = malloc(BUFFER_SIZE);
  posix_memalign((void **)&conn->send_region, sysconf(_SC_PAGESIZE), COMPUTE_BUFFER_SIZE);
  posix_memalign((void **)&conn->recv_region, sysconf(_SC_PAGESIZE), COMPUTE_BUFFER_SIZE);
  posix_memalign((void **)&conn->local_cache, sysconf(_SC_PAGESIZE), COMPUTE_BUFFER_SIZE);
  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));
  //conn->send_msg->data = malloc(PAGE_SIZE);
  //conn->recv_msg->data = malloc(PAGE_SIZE);

  /*TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_region, 
    COMPUTE_BUFFER_SIZE, 
    0));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_region, 
    COMPUTE_BUFFER_SIZE, 
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

int on_addr_resolved(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct connection *conn;

  printf("address resolved.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;
  conn->num_completions = 0;
  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  register_memory(conn);
  post_receives(conn);

  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}
void send_write_request(struct connection *conn, int addr){
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memcpy(conn->send_msg->data, conn->local_cache + addr * PAGE_SIZE, PAGE_SIZE);
  conn->send_msg->addr = addr;
  // snprintf(conn->send_region, PAGE_SIZE, "writing from client side to server side with count %d", count);
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

void send_read_request(struct connection *conn, int addr){
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  conn->send_msg->addr = addr;
  // snprintf(conn->send_region, PAGE_SIZE, "%d", conn->random_access_read[addr]);
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
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
  
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV)
    { 
      conn->num_completions++;
      // printf("received message: %s", conn->recv_region); 
      if (conn->num_completions < NR_LOOPS * 2 + 1){
        post_receives(conn);
        /* send write request before, we receive a write ACK and send read request*/
        if (conn->send_state == SS_WRITE_SENT) {
          send_read_request(conn, conn->random_access_read[conn->num_completions]);
          // printf("send read request\n");
        }
        /* send read request before, we receive data for read and send write request*/
        else if (conn->send_state == SS_READ_SENT) {
          /* local aggregation */
          struct message *recv = conn->recv_msg;
          for (int i = 0; i < PAGE_SIZE; i++){
            conn->local_cache[i] += recv->data[i];
          }
          send_write_request(conn, conn->random_access_write[conn->num_completions]);
          // printf("send write request\n");
        }
      }
    }
  else if (wc->opcode == IBV_WC_SEND)
    { 
      // printf("send completed successfully.\n");
    }
  else
    die("on_completion: completion isn't a send or a receive.");

  if (conn->num_completions >= NR_LOOPS * 2 + 1)
  {
    gettimeofday(&conn->tv_end, NULL);
    printf("overall time is %f\n", (conn->tv_end.tv_sec - conn->tv_begin.tv_sec) + (conn->tv_end.tv_usec - conn->tv_begin.tv_usec)/1000000.0);
    rdma_disconnect(conn->id);
  }
  
    
}


int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  gettimeofday(&conn->tv_begin, NULL);
  send_write_request(conn, conn->random_access_write[0]);
  // printf("send write request\n");
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);
  //free(conn->send_msg->data);
  //free(conn->recv_msg->data);
  free(conn->send_msg);
  free(conn->recv_msg);

  free(conn);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("route resolved.\n");

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_connect(id, &cm_params));

  return 0;
}

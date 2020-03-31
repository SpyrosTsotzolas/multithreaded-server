//Spuros Tsotzolas 3099
//Iliana Vlachou  2411

/* server.c

   Sample code of
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <sys/time.h>


#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define QUEUE_SIZE               100        // our queue size
#define NUM_THREADS               20        // the number of consumers threads


/* Definition of the request_describer.
   A struct that contains:
            -- the file decriptor of incomming connection
            -- the start time of connection
*/
typedef struct request_describer{
    int connection_fd;
    struct timeval start_time;
}Request_describer;


// some archetypes..
void *consumer(void *argv);
Request_describer remove_from_queue();
void process_request(const int socket_fd);


// we need 4 mutex..
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t time_statistics = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rw_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t socket_lock = PTHREAD_MUTEX_INITIALIZER;


// we need 4 conditionals
pthread_cond_t non_full_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t non_empty_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t get_function = PTHREAD_COND_INITIALIZER;
pthread_cond_t put_function = PTHREAD_COND_INITIALIZER;


// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation;


// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];
  char value[VALUE_SIZE];
} Request;


// our global values
int head = 0;
int tail = 0;
int countReader=0;
int countWriter=0;
struct timeval total_waiting_time;
struct timeval total_service_time;
int completed_requests = 0;


// Definition array of consumers threads
pthread_t consumers[NUM_THREADS];


// Definition of my queue
Request_describer my_queue[QUEUE_SIZE];


// Definition of the database.
KISSDB *db = NULL;


/* @ isEmpty() - check if my queue is empty
   @ return 1 if it is empty or 0 if it is not
*/
int isEmpty(){
    if(head==tail){
        return 1;
    }
    else{
        return 0;
    }
}


/* @ isFull() - check if my queue is full
   @ return 1 if it is full or 0 if it is not
*/
int isFull(){
    if((head>tail && head-tail==1) || (head<tail && tail-head==QUEUE_SIZE-1)){
        return 1;
    }
    else{
        return 0;
    }
}


// @ create_consumers() - it just creates our consumers-threads, nothing more...
void create_consumers(){
  int i;
  for(i=0; i<NUM_THREADS; i++){
      pthread_create(&consumers[i], NULL, consumer, NULL);
  }
}


/* @ consumer() - it is the main function that the consumers-threads
                  use in order to make their 'job'.. @remove_from_queue()
                  and @process_request are called in it
*/
void *consumer(void *argv){
    int fd;
    Request_describer req;
    struct timeval export_time, after_survice;        // to calculate the service_time for each request
    long seconds_st, microseconds_st, seconds_wt, microseconds_wt;

    while(1){
        // we must lock since our queue is shared
        pthread_mutex_lock(&queue_lock);

        while(isEmpty()){
            pthread_cond_wait(&non_empty_queue, &queue_lock);   // the threads wait here as long as
        }                                                       // there not requests for survice

        if(isFull()){
            req = remove_from_queue();
            pthread_cond_signal(&non_full_queue);             // we send sygnal to producer to inform him
        }                                                     // that there is empty space in queue
        else{
            req = remove_from_queue();
        }
        pthread_mutex_unlock(&queue_lock);   // now we unlock because we want a lots threads to call the process_request()

        gettimeofday(&export_time, NULL);    // export_time from queue
        seconds_wt = (export_time.tv_sec - req.start_time.tv_sec);   // we calculate seconds and microseconds
        microseconds_wt = (export_time.tv_sec - req.start_time.tv_sec)*1000000 + (export_time.tv_usec - req.start_time.tv_usec);
        fd = req.connection_fd;

        //gettimeofday(&before_survice, NULL);
        process_request(fd);
        gettimeofday(&after_survice, NULL);
        seconds_st = (after_survice.tv_sec - req.start_time.tv_sec);   // we calculate the seconds and microseconds, which elapsed until the request was surviced
        microseconds_st = (after_survice.tv_sec - req.start_time.tv_sec)*1000000 + (after_survice.tv_usec - req.start_time.tv_usec);

        printf("Request with connection fd %d was serviced in %ld sec and %ld usec\n", fd, seconds_st, microseconds_st);
        printf("Request with connection fd %d waited in queue %ld sec and %ld usec\n", fd, seconds_wt, microseconds_wt);

        pthread_mutex_lock(&time_statistics);
        total_service_time.tv_sec = total_service_time.tv_sec + seconds_st;
        total_service_time.tv_usec = total_service_time.tv_usec + microseconds_st;
        total_waiting_time.tv_sec = total_waiting_time.tv_sec + seconds_wt;
        total_waiting_time.tv_usec = total_waiting_time.tv_usec + microseconds_wt;

        completed_requests++;
        pthread_mutex_unlock(&time_statistics);

        close(fd);
      }

    return NULL;
}


// @ add_to_queue - adds new requests in our queue..
void add_to_queue(int new_fd){
    my_queue[tail].connection_fd = new_fd;
    gettimeofday(&my_queue[tail].start_time,NULL);

    tail++;
    if(tail>QUEUE_SIZE-1){
        tail = 0;
    }
}


/* @ remove_from_queue - removes requests from our queue and also
    calculates the waiting time for the specific request.. @consumer() calls it
*/
Request_describer remove_from_queue(){
    Request_describer req;

    req = my_queue[head];

    head++;
    if(head>QUEUE_SIZE-1){
        head = 0;
    }
    return req;
}


void terminate_communication(){
    long average_waiting_time_sec, average_waiting_time_usec, average_service_time_sec, average_service_time_usec;

    average_waiting_time_sec = total_waiting_time.tv_sec/completed_requests;
    average_waiting_time_usec = total_waiting_time.tv_usec/completed_requests;
    average_service_time_sec = total_service_time.tv_sec/completed_requests;
    average_service_time_usec = total_service_time.tv_usec/completed_requests;

    printf("\n------------------Results-----------------------\n");
    printf("The server surviced %d requests\n", completed_requests);
    printf("The average waiting time in queue was : %ld sec and %ld usec\n", average_waiting_time_sec, average_waiting_time_usec);
    printf("The average service time for all requests : %ld sec and %ld usec\n", average_service_time_sec, average_service_time_usec);
    printf("--------------------------------------------------\n");
    printf("The work done...\n");

    exit(1);  // all threads are destroyed..

}


/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;

  // Check arguments.
  if (!buffer)
    return NULL;

  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {pthread_mutex_unlock(&socket_lock);
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }

  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }

  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}


/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
 void process_request(const int socket_fd) {
   char response_str[BUF_SIZE], request_str[BUF_SIZE];
     int numbytes = 0;
     Request *request = NULL;

     // Clean buffers.
     memset(response_str, 0, BUF_SIZE);
     memset(request_str, 0, BUF_SIZE);

     // receive message.
     // we also lock here because we don't want 2 or more threads to read at the same time..
     pthread_mutex_lock(&socket_lock);
     numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
     pthread_mutex_unlock(&socket_lock);


     // parse the request.
     if (numbytes) {
       request = parse_request(request_str);

       if (request) {
         switch (request->operation) {
           case GET:
             // we lock to protect our global value countReader
             pthread_mutex_lock(&rw_mutex);

             while(countWriter>0){
                pthread_cond_wait(&put_function, &rw_mutex);  // we must wait because a put function is run
             }                                                // we will receive sygnal to go on when none put function is run

             countReader++;

             pthread_mutex_unlock(&rw_mutex);  // now we unlock to permit many get functions to run at the same time

             // Read the given key from the database.
             if (KISSDB_get(db, request->key, request->value))
               sprintf(response_str, "GET ERROR\n");
             else
               sprintf(response_str, "GET OK: %s\n", request->value);

             // we lock again to protect our global value countReader
             pthread_mutex_lock(&rw_mutex);

             if(--countReader==0){
                pthread_cond_signal(&get_function); // there is not any get function.. so we must
                                                    // send signal to put_function to go on if threads wait there
             }
             pthread_mutex_unlock(&rw_mutex);
             break;

           case PUT:
              // we lock here and unlock in the end so that only one put function to run every time
              pthread_mutex_lock(&rw_mutex);
              while(!(countReader==0)){
                  pthread_cond_wait(&get_function, &rw_mutex); // we must wait until we receive sygnal from get_function
              }                                                // that we can continue

              countWriter++;

             // Write the given key/value pair to the database.
             if (KISSDB_put(db, request->key, request->value))
               sprintf(response_str, "PUT ERROR\n");
             else
               sprintf(response_str, "PUT OK\n");

             if(--countWriter==0){
                pthread_cond_broadcast(&put_function);        // there is not any put function.. so we must
             }                                                // send signal to get_function to go on all the threads that may wait there

             pthread_mutex_unlock(&rw_mutex);
             break;
           default:
             // Unsupported operation.
             sprintf(response_str, "UNKOWN OPERATION\n");
         }
         // Reply to the client.
         write_str_to_socket(socket_fd, response_str, strlen(response_str));

         if (request)
           free(request);
         request = NULL;
         return;
       }
     }
     // Send an Error reply to the client.
     sprintf(response_str, "FORMAT ERROR\n");
     write_str_to_socket(socket_fd, response_str, strlen(response_str));

 }


/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  signal(SIGTSTP, terminate_communication);  // it waits for ctr+z signal

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);

  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);

  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");

  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }

  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  // create the threads
  create_consumers();

  // main loop: wait for new connection/requests
  while (1) {
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }

    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));

    //we lock to protect our queue
    pthread_mutex_lock(&queue_lock);

    while(isFull()){
        pthread_cond_wait(&non_full_queue, &queue_lock);  // producer waits here as long as he receives a signal from consumer.
    }                                                     // that signal will signify that there is a empty space in queue

    // we enter the request in our queue
    add_to_queue(new_fd);
    // send signal to our consumer to inform him that there is request for survice
    pthread_cond_signal(&non_empty_queue);
    pthread_mutex_unlock(&queue_lock);

  }

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;
  return 0;
}

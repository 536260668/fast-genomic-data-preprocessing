#ifndef KTHREAD_H_
#define KTHREAD_H_

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <stdio.h>
#include <linux/unistd.h>

typedef struct { //every thread have a ktf_worker_t
	struct kt_for_t *t;
	long i; // can be see as the thread number
	int if_finish;
} ktf_worker_t;

typedef struct kt_for_t {
	int n_threads;
	long n; // the number of read [pair]
	ktf_worker_t *w;

	void (*func)(void *, long, int);

	void *data;
	pthread_mutex_t mutex;
	pthread_cond_t work_cv;
	pthread_cond_t main_cv;
	pthread_t *tid;
	int if_exit;
} kt_for_t;

typedef struct {
	struct ktp_t *pl;
	int64_t index;
	int step;
	void *data;
} ktp_worker_t;

typedef struct ktp_t {
	void *shared;

	void *(*func)(void *, int, void *);

	int64_t index;
	int n_workers, n_steps;
	ktp_worker_t *workers;
	pthread_mutex_t mutex;
	pthread_cond_t cv;
} ktp_t;

#ifdef __cplusplus
extern "C" {
#endif

/************
 * kt_for() *
 ************/
static inline long steal_work(kt_for_t *t);

static void *ktf_worker(void *data);

void kt_for(int n_threads, void (*func)(void *, long, int), void *data, long n, int exit_flag, kt_for_t *kt);

/*****************
 * kt_pipeline() *
 *****************/
static void *ktp_worker(void *data);

void kt_pipeline(int n_threads, void *(*func)(void *, int, void *), void *shared_data, int n_steps);


#ifdef __cplusplus
}
#endif

#endif //KTHREAD_H_

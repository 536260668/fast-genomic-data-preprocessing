#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <stdio.h>
#include "kthread.h"
// pid_t gettid(void) { return syscall(__NR_gettid); }

/************
 * kt_for() *
 ************/

static inline long steal_work(kt_for_t *t)
{
	int i, min_i = -1;
	long k, min = LONG_MAX;
	for (i = 0; i < t->n_threads; ++i)
		if (min > t->w[i].i) min = t->w[i].i, min_i = i;
	k = __sync_fetch_and_add(&t->w[min_i].i, t->n_threads);
	return k >= t->n? -1 : k;
}

static void *ktf_worker(void *data)
{
	ktf_worker_t *w = (ktf_worker_t*)data;
	kt_for_t *t = (kt_for_t*)w->t;
	long i;
	for(;;) {
		pthread_mutex_lock(&t->mutex);
		for(;;) {
			if ((!w->if_finish && t->func != 0) || t->if_exit) break;
			pthread_cond_wait(&t->work_cv, &t->mutex);   //waiting signal
		}
		pthread_mutex_unlock(&t->mutex);

		if (t->if_exit) break;

		// working
		for (;;) {  // to make every thread process w->t->n / n_threads reads
			i = __sync_fetch_and_add(&w->i, w->t->n_threads);
			if (i >= w->t->n) break;
			w->t->func(w->t->data, i, w - w->t->w);
		}
		while ((i = steal_work(w->t)) >= 0)
			w->t->func(w->t->data, i, w - w->t->w);

		w->if_finish = 1;
		pthread_cond_broadcast(&t->main_cv);
	}
	pthread_exit(0);
}

void kt_for(int n_threads, void (*func)(void*,long,int), void *data, long n, int exit_flag, kt_for_t *kt)
{
	int i;
	kt_for_t *t = (kt_for_t*)kt;

	if (!exit_flag){    // thread_pool_init
		if (t->func == 0) {
			if (n_threads < 1) n_threads = 1;

			pthread_mutex_init(&t->mutex, 0);
			pthread_cond_init(&t->work_cv, 0);
			pthread_cond_init(&t->main_cv, 0);

			t->n_threads = n_threads, t->if_exit = 0;
			t->w = (ktf_worker_t*)malloc(n_threads * sizeof(ktf_worker_t));
			t->tid = (pthread_t*)malloc(n_threads * sizeof(pthread_t));
			for (i = 0; i < n_threads; ++i)	t->w[i].t = t;
			for (i = 0; i < n_threads; ++i) pthread_create(&t->tid[i], 0, ktf_worker, &t->w[i]);
		}

		// wake up threads to work
		pthread_mutex_lock(&t->mutex);
		t->func = func, t->data = data, t->n = n;
		for (i = 0; i < t->n_threads; ++i) t->w[i].i = i, t->w[i].if_finish = 0;
		pthread_cond_broadcast(&t->work_cv);
		pthread_mutex_unlock(&t->mutex);

		// waiting threads to finish
		pthread_mutex_lock(&t->mutex);
		for(;;) {
			for (i = 0; i < t->n_threads; ++i) {
				if (t->w[i].if_finish == 0) break;
			}
			if (i == t->n_threads) break;
			pthread_cond_wait(&t->main_cv, &t->mutex);
		}
		pthread_mutex_unlock(&t->mutex);
	}
	else {  //thread_pool_destory
		t->if_exit = 1;
		pthread_cond_broadcast(&t->work_cv);

		for (i = 0; i < n_threads; ++i) pthread_join(t->tid[i], 0);

		free(t->tid);
		free(t->w);
		pthread_mutex_destroy(&t->mutex);
		pthread_cond_destroy(&t->work_cv);
		pthread_cond_destroy(&t->main_cv);
	}
}

/*****************
 * kt_pipeline() *
 *****************/

static void *ktp_worker(void *data)
{
	ktp_worker_t *w = (ktp_worker_t*)data;
	ktp_t *p = w->pl;
	int i;

	while (w->step < p->n_steps) {
		// test whether we can kick off the job with this worker
		pthread_mutex_lock(&p->mutex);
		for(;;) {
			// test whether another worker is doing the same step
			for (i = 0; i < p->n_workers; ++i) {
				//if (w == &p->workers[i]) continue; // ignore itself ==> NO, it will never break when workers[i]==w
				if (p->workers[i].index < w->index && p->workers[i].step <= w->step)
					break;
			}
			if (i == p->n_workers) break; // no workers with smaller indices are doing w->step or the previous steps
			pthread_cond_wait(&p->cv, &p->mutex);
		}
		pthread_mutex_unlock(&p->mutex);

		// working on w->step
		w->data = p->func(p->shared, w->step, w->step? w->data : 0); // for the first step, input is NULL

		// update step and let other workers know
		pthread_mutex_lock(&p->mutex);
		// fprintf(stderr, "[M::%s][PID%d]wstep:%d windex:%ld pindex:%ld\n", __func__, gettid(), w->step, w->index, p->index);
		w->step = w->step == p->n_steps - 1 || w->data ? (w->step + 1) % p->n_steps : p->n_steps;
		if (w->step == 0) w->index = p->index++;
		pthread_cond_broadcast(&p->cv);
		pthread_mutex_unlock(&p->mutex);
	}
	pthread_exit(0);
}

void kt_pipeline(int n_threads, void *(*func)(void*, int, void*), void *shared_data, int n_steps)
{
	ktp_t aux;
	pthread_t *tid;
	int i;

	n_threads = n_steps > 16 ? 16 : n_steps;
	if (n_threads < 1) n_threads = 1;

	aux.n_workers = n_threads;
	aux.n_steps = n_steps;
	aux.func = func;
	aux.shared = shared_data;
	aux.index = 0;
	pthread_mutex_init(&aux.mutex, 0);
	pthread_cond_init(&aux.cv, 0);

	aux.workers = (ktp_worker_t*)malloc(n_threads * sizeof(ktp_worker_t));
	for (i = 0; i < n_threads; ++i) {
		ktp_worker_t *w = &aux.workers[i];
		w->step = 0; w->pl = &aux; w->data = 0;
		w->index = aux.index++;
	}

	tid = (pthread_t*)malloc(n_threads * sizeof(pthread_t));
	for (i = 0; i < n_threads; ++i) pthread_create(&tid[i], 0, ktp_worker, &aux.workers[i]);
	for (i = 0; i < n_threads; ++i) pthread_join(tid[i], 0);

	free(tid);
	free(aux.workers);
	pthread_mutex_destroy(&aux.mutex);
	pthread_cond_destroy(&aux.cv);
}

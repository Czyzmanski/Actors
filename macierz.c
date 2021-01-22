#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "cacti.h"

#define MESSAGES_TYPES 5
#define MSG_INIT_REQUEST 1
#define MSG_INIT 2
#define MSG_COMPUTE 3
#define MSG_FINISH 4

size_t k, n;

typedef struct pair {
    int val;
    int time;
} pair_t;

typedef struct init_data {
    size_t col;
    int val;
    role_t *role;
} init_data_t;

typedef struct fact_comp {
    size_t col;
    int val;
    actor_id_t id_self;
    actor_id_t parent;
    role_t *role;
    init_data_t on_init_request_data;
} fact_comp_t;

pair_t **matrix;

typedef struct partial_sum {
    size_t row;
    long sum;
} partial_sum_t;

partial_sum_t *partial_sum;

void on_hello(void **stateptr, size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(fact_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }

    actor_id_t *parent = data;
    fact_comp_t *matrix_comp = *stateptr;
    matrix_comp->id_self = actor_id_self();
    matrix_comp->parent = *parent;

    message_t init_request;
    init_request.message_type = MSG_INIT_REQUEST;
    init_request.nbytes = sizeof(actor_id_t);
    init_request.data = &matrix_comp->id_self;

    int err;
    if ((err = send_message(*parent, init_request))) {
        //TODO: error handling
    }
}

void on_init_request(void **stateptr, size_t nbytes, void *data) {
    actor_id_t *requester = data;

    fact_comp_t *matrix_comp = *stateptr;
    matrix_comp->on_init_request_data.col = matrix_comp->col - 1;
    matrix_comp->on_init_request_data.val = 0;
    matrix_comp->on_init_request_data.role = matrix_comp->role;

    message_t init;
    init.message_type = MSG_INIT;
    init.nbytes = sizeof(init_data_t);
    init.data = &matrix_comp->on_init_request_data;

    int err;
    if ((err = send_message(*requester, init))) {
        //TODO: error handling
    }
}

void on_init(void **stateptr, size_t nbytes, void *data) {
    if (*stateptr == NULL) {
        *stateptr = malloc(sizeof(fact_comp_t));
        if (*stateptr == NULL) {
            exit(EXIT_FAILURE);
        }
    }

    init_data_t *init_data = data;
    fact_comp_t *matrix_comp = *stateptr;
    matrix_comp->col = init_data->col;
    matrix_comp->val = init_data->val;
    matrix_comp->role = init_data->role;
    matrix_comp->id_self = actor_id_self();

    int err;
    if (matrix_comp->col == 0) {
        message_t compute;
        compute.message_type = MSG_COMPUTE;
        compute.nbytes = sizeof(partial_sum_t);
        compute.data = &partial_sum[0];

        if ((err = send_message(actor_id_self(), compute))) {
            //TODO: error handling
        }
    }
    else {
        message_t spawn;
        spawn.message_type = MSG_SPAWN;
        spawn.nbytes = sizeof(role_t);
        spawn.data = matrix_comp->role;

        if ((err = send_message(actor_id_self(), spawn))) {
            //TODO: error handling
        }
    }
}

void on_compute(void **stateptr, size_t nbytes, void *data) {
    fact_comp_t *matrix_comp = *stateptr;
    partial_sum_t *partial_comp = data;
    size_t curr_row = partial_comp->row;
    pair_t matrix_cell = matrix[curr_row][matrix_comp->col];

    usleep(matrix_cell.time * 1000);

    matrix_comp->val = matrix_cell.val;
    partial_comp->sum += matrix_comp->val;

    int err;
    if (matrix_comp->col == n - 1) {
        printf("%ld\n", partial_comp->sum);
    }
    else {
        message_t compute;
        compute.message_type = MSG_COMPUTE;
        compute.nbytes = sizeof(partial_sum_t);
        compute.data = partial_comp;

        if ((err = send_message(matrix_comp->parent, compute))) {
            //TODO: error handling
        }
    }

    if (matrix_comp->col == 0) {
        if (curr_row == k - 1) {
            message_t finish;
            finish.message_type = MSG_FINISH;
            finish.nbytes = 0;
            finish.data = NULL;

            if ((err = send_message(matrix_comp->id_self, finish))) {
                //TODO: error handling
            }
        }
        else {
            message_t compute;
            compute.message_type = MSG_COMPUTE;
            compute.nbytes = sizeof(partial_sum_t);
            compute.data = &partial_sum[curr_row + 1];

            if ((err = send_message(matrix_comp->id_self, compute))) {
                //TODO: error handling
            }
        }
    }
}

void on_finish(void **stateptr, size_t nbytes, void *data) {
    fact_comp_t *matrix_comp = *stateptr;
    int err;

    if (matrix_comp->col < n - 1) {
        message_t finish;
        finish.message_type = MSG_FINISH;
        finish.nbytes = 0;
        finish.data = NULL;

        if ((err = send_message(matrix_comp->parent, finish))) {
            //TODO: error handling
        }
    }

    message_t go_die;
    go_die.message_type = MSG_GODIE;
    go_die.nbytes = 0;
    go_die.data = NULL;

    free(matrix_comp);
    *stateptr = NULL;

    if ((err = send_message(actor_id_self(), go_die))) {
        //TODO: error handling
    }
}

int main(){
    scanf("%zd\n%zd", &k, &n);

    matrix = malloc(k * sizeof(pair_t *));
    if (matrix == NULL) {
        exit(EXIT_FAILURE);
    }

    partial_sum = malloc(k * sizeof(partial_sum_t));
    if (partial_sum == NULL) {
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < k; i++) {
        partial_sum[i].row = i;
        partial_sum[i].sum = 0;

        matrix[i] = malloc(n * sizeof(pair_t));
        if (matrix[i] == NULL) {
            exit(EXIT_FAILURE);
        }

        for (size_t j = 0; j < n; j++) {
            scanf("%d %d", &matrix[i][j].val, &matrix[i][j].time);
        }
    }

    act_t acts[] = {on_hello, on_init_request, on_init, on_compute, on_finish};
    role_t role;
    role.nprompts = MESSAGES_TYPES;
    role.prompts = acts;

    actor_id_t first_actor;
    int err;
    if ((err = actor_system_create(&first_actor, &role))) {
        //TODO: error handling
        return err;
    }

    init_data_t init_data;
    init_data.col = n - 1;
    init_data.val = 0;
    init_data.role = &role;

    message_t start_computation;
    start_computation.message_type = MSG_INIT;
    start_computation.nbytes = sizeof(init_data_t);
    start_computation.data = &init_data;

    if ((err = send_message(first_actor, start_computation))) {
        //TODO: error handling
        return err;
    }
    actor_system_join(first_actor);

	return 0;
}

#include <stdio.h>
#include <stdlib.h>
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
    size_t row;
    size_t col;
    int val;
    role_t *role;
} init_data_t;

typedef struct matrix_comp {
    size_t row;
    size_t col;
    int val;
    actor_id_t id_self;
    actor_id_t parent;
    role_t *role;
    init_data_t on_init_request_data;
} matrix_comp_t;

pair_t **matrix;

int *last_val;

void on_hello(void **stateptr, size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(matrix_comp_t));
    if (*stateptr == NULL) {
        exit(EXIT_FAILURE);
    }
    matrix_comp_t *matrix_comp = *stateptr;
    actor_id_t *parent = data;
    matrix_comp->id_self = actor_id_self();
    matrix_comp->parent = *parent;

    message_t init_request;
    init_request.message_type = MSG_INIT_REQUEST;
    init_request.nbytes = sizeof(actor_id_t);
    init_request.data = &matrix_comp->id_self;

    int err = send_message(*parent, init_request);
    if (err != 0) {
        //TODO: error handling
    }
}

void on_init_request(void **stateptr, size_t nbytes, void *data) {
    matrix_comp_t *matrix_comp = *stateptr;
    matrix_comp->on_init_request_data.row = matrix_comp->row;
    matrix_comp->on_init_request_data.col = matrix_comp->col + 1;
    matrix_comp->on_init_request_data.val = 0;
    matrix_comp->on_init_request_data.role = matrix_comp->role;

    message_t init;
    init.message_type = MSG_INIT;
    init.nbytes = sizeof(init_data_t);
    init.data = &matrix_comp->on_init_request_data;

    actor_id_t *receiver = data;
    int err = send_message(*receiver, init);
    if (err != 0) {
        //TODO: error handling
    }
}

void on_init(void **stateptr, size_t nbytes, void *data) {
    matrix_comp_t *matrix_comp = *stateptr;
    init_data_t *init_data = data;
    matrix_comp->row = init_data->row;
    matrix_comp->col = init_data->col;
    matrix_comp->val = 0;
    matrix_comp->role = init_data->role;

    int err;
    if (matrix_comp->col == n - 1) {
        message_t compute;
        compute.message_type = MSG_COMPUTE;
        compute.nbytes = sizeof(int);
        compute.data = &matrix_comp->val;
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

}

void on_finish(void **stateptr, size_t nbytes, void *data) {

}

int main(){
    scanf("%zd\n%zd", &k, &n);

    matrix = malloc(k * sizeof(pair_t *));
    if (matrix == NULL) {
        exit(EXIT_FAILURE);
    }
    last_val = malloc(k * sizeof(int));

    for (size_t i = 0; i < k; i++) {
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
    int err = actor_system_create(&first_actor, &role);
    if (err != 0) {
        //TODO: error handling
        return err;
    }

    init_data_t init_data;
    init_data.row = 0;
    init_data.col = 0;
    init_data.val = 0;
    init_data.role = &role;

    message_t start_computation;
    start_computation.message_type = MSG_INIT;
    start_computation.nbytes = sizeof(init_data_t);
    start_computation.data = &init_data;

    err = send_message(first_actor, start_computation);
    if (err != 0) {
        //TODO: error handling
        return err;
    }
    actor_system_join(first_actor);

	return 0;
}

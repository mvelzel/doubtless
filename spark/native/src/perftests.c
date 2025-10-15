#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdint.h>

#include "vector.h"
#include "utils.h"
#include "dictionary.h"
#include "bdd.h"

static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                                'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                                'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                                'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                                'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
                                'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                                'w', 'x', 'y', 'z', '0', '1', '2', '3',
                                '4', '5', '6', '7', '8', '9', '+', '/'};
static int mod_table[] = {0, 2, 1};


char *base64_encode(const unsigned char *data,
                    size_t input_length) {
    size_t output_length = 4 * ((input_length + 2) / 3) + 1;

    char *encoded_data = malloc(output_length);
    if (encoded_data == NULL) return NULL;

    for (int i = 0, j = 0; i < input_length;) {

        uint32_t octet_a = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_b = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_c = i < input_length ? (unsigned char)data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[input_length % 3]; i++)
        encoded_data[output_length - 2 - i] = '=';

    encoded_data[output_length - 1] = '\0';

    return encoded_data;
}

char* exprs[] = {
    "1",
    "x=1",
    "(x=1|y=1)",
    "(x=1&y=1)|(z=5)",
    "(c=0&!b=0)",
    "x=1|(y=1 & x=2)",
    "(!(c=0|a=1)|!(b=2))",
    "(z=1)&!((x=1)&((y=1|y=2)&x=2))",
    "(x=8|x=2|x=4|x=3|x=9|(x=6|q=4&p=6)|x=7|x=1|x=5)",
    "(x=1&x=2|x=3&x=4|x=5&x=6|x=7&x=8|y=4)"
};
bdd* bdds[10];

static void test_instantiate() {
    char* _errmsg = NULL;
    bdd* bdd = NULL;

    int instantiations = 10000000;
    int size = sizeof(exprs) / sizeof(exprs[0]);

    struct timeval stop, start;
    gettimeofday(&start, NULL);

    for (int i = 0; i < instantiations; i++) {
        bdd = create_bdd(BDD_DEFAULT, exprs[i % size], &_errmsg, 0);
        V_rva_node_free(&bdd->tree);
        free(bdd);
        bdd = NULL;
    }

    gettimeofday(&stop, NULL);

    printf("Elapsed time: %lums\n", (stop.tv_sec - start.tv_sec) * 1000 + (stop.tv_usec - start.tv_usec) / 1000);
}

char* left_exprs[] = {
    "x=1",
    "x=1|(!y=3)",
    "(x=2&z=4)"
};
bdd* left_bdds[3];

char* right_exprs[] = {
    "x=1",
    "x=1|(!y=3)",
    "(x=2&z=4)"
};
bdd* right_bdds[3];

static void test_combine() {
    char* _errmsg = NULL;

    int repeat = 1000000;

    int left_size = sizeof(left_exprs) / sizeof(left_exprs[0]);
    for (int i = 0; i < left_size; i++) 
        left_bdds[i] = create_bdd(BDD_DEFAULT, left_exprs[i], &_errmsg, 0);

    int right_size = sizeof(right_exprs) / sizeof(right_exprs[0]);
    for (int i = 0; i < right_size; i++) 
        right_bdds[i] = create_bdd(BDD_DEFAULT, right_exprs[i], &_errmsg, 0);

    struct timeval stop, start;
    gettimeofday(&start, NULL);

    for (int r = 0; r < repeat; r++) {
        for (int i = 0; i < left_size; i++) {
            for (int j = 0; j < right_size; j++) {
                bdd* res;

                res = bdd_operator('&', BY_APPLY, left_bdds[i], right_bdds[j], &_errmsg);
                V_rva_node_free(&res->tree);
                free(res);
                res = NULL;

                res = bdd_operator('|', BY_APPLY, left_bdds[i], right_bdds[j], &_errmsg);
                V_rva_node_free(&res->tree);
                free(res);
                res = NULL;
            }
        }
    }

    gettimeofday(&stop, NULL);

    for (int i = 0; i < left_size; i++) 
        free(left_bdds[i]);
    for (int i = 0; i < right_size; i++) 
        free(right_bdds[i]);

    printf("Elapsed time: %lums\n", (stop.tv_sec - start.tv_sec) * 1000 + (stop.tv_usec - start.tv_usec) / 1000);
}

static void test_negate() {
    char* _errmsg = NULL;

    int repeat = 1000000;

    int size = sizeof(exprs) / sizeof(exprs[0]);
    for (int i = 0; i < size; i++) 
        bdds[i] = create_bdd(BDD_DEFAULT, exprs[i], &_errmsg, 0);

    struct timeval stop, start;
    gettimeofday(&start, NULL);

    for (int r = 0; r < repeat; r++) {
        for (int i = 0; i < size; i++) {
            bdd* res;

            res = bdd_operator('!', BY_APPLY, bdds[i], NULL, &_errmsg);
            V_rva_node_free(&res->tree);
            free(res);
            res = NULL;
        }
    }

    gettimeofday(&stop, NULL);

    for (int i = 0; i < size; i++) 
        free(bdds[i]);

    printf("Elapsed time: %lums\n", (stop.tv_sec - start.tv_sec) * 1000 + (stop.tv_usec - start.tv_usec) / 1000);
}

static void print_info(char* info) {
    printf("[info] \033[0;32m%s\033[0m\n", info);
}

static void big_bdd() {
    char* _errmsg = NULL;
    char* op = "&";

    int repeat = 41663;

    bdd* res = NULL;
    for (int r = 0; r < repeat; r++) {
        char expr[100];
        sprintf(expr, "x=%d", r);
        char* expr_ptr = expr;
        bdd* new_bdd;
        if (!(new_bdd = create_bdd(BDD_DEFAULT, expr_ptr, &_errmsg, 0))) {
            printf("create_bdd: error: %s ",(_errmsg ? _errmsg : "NULL"));
        }

        if (res == NULL) {
            res = new_bdd;
        } else {
            bdd* new_res;

            if (!(new_res = bdd_operator(*op, BY_APPLY, res, new_bdd, &_errmsg))) {
                printf("bdd_operator: error: %s ",(_errmsg ? _errmsg : "NULL"));
            }

            printf("Inter size: %d, repeat: %d\n", new_res->bytesize, r);

            V_rva_node_free(&new_bdd->tree);
            free(new_bdd);
            new_bdd = NULL;

            V_rva_node_free(&res->tree);
            free(res);
            res = new_res;
        }

        if (_errmsg != NULL) {
            printf("%s", _errmsg);
        }
    }

    printf("Total size: %d\n", res->bytesize);

    char* encoded_data = base64_encode((unsigned char*) res, res->bytesize);

    FILE *fptr;
    fptr = fopen("1mb_bdd.txt", "w+");
    fprintf(fptr, "%s", encoded_data);
    fclose(fptr);
    fptr = NULL;

    free(encoded_data);
    encoded_data = NULL;

    V_rva_node_free(&res->tree);
    free(res);
    res = NULL;
}

int main() {
    big_bdd();
    //print_info("A BDD");
    //test_instantiate();
    //print_info("- should be able to quickly be instantiated");
    //test_combine();
    //print_info("- should be able to quickly be combined");
    //test_negate();
    //print_info("- should be able to quickly be negated");
}

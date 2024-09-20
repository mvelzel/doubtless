#include "com_doubtless_test_Test.h"

#include "test_config.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "vector.h"
#include "utils.h"
#include "dictionary.h"
#include "bdd.h"

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_test_Test_createBdd
(JNIEnv* env, jobject obj, jstring expr) {
    const char* expr_chars = (*env)->GetStringUTFChars(env, expr, 0);
    char* _errmsg = NULL;

    bdd* bdd = NULL;
    if (!(bdd = create_bdd(BDD_DEFAULT, (char*)expr_chars, &_errmsg, 0))) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        (*env)->ReleaseStringUTFChars(env, expr, expr_chars);

        (*env)->ThrowNew(env, error_class, (_errmsg ? _errmsg : "NULL"));

        return 0;
    }


    (*env)->ReleaseStringUTFChars(env, expr, expr_chars);

    jbyteArray ret = (*env)->NewByteArray(env, bdd->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, bdd->bytesize, (jbyte*)bdd);
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_doubtless_test_Test_bdd2string
(JNIEnv* env, jobject obj, jbyteArray bdd_arr) {
    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);

    jbyte* bytes = (*env)->GetByteArrayElements(env, bdd_arr, NULL);

    bdd2string(pbuff, (bdd*)bytes, 1);
    jstring res = (*env)->NewStringUTF(env, pbuff->buffer);
    pbuff_free(pbuff);

    (*env)->ReleaseByteArrayElements(env, bdd_arr, bytes, 0);

    return res;
}

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_test_Test_bddOperator
(JNIEnv* env, jobject obj, jstring operator, jbyteArray left_bdd_arr, jbyteArray right_bdd_arr) {
    const char* operator_chars = (*env)->GetStringUTFChars(env, operator, 0);
    jbyte* left_bdd_bytes = (*env)->GetByteArrayElements(env, left_bdd_arr, NULL);
    jbyte* right_bdd_bytes = NULL;
    if (*operator_chars == '&' || *operator_chars == '|')
        right_bdd_bytes = (*env)->GetByteArrayElements(env, right_bdd_arr, NULL);
    char* _errmsg = NULL;
    
    bdd* res_bdd = NULL;
    if (!(res_bdd = bdd_operator(*operator_chars, BY_APPLY, (bdd*)left_bdd_bytes, (bdd*)right_bdd_bytes, &_errmsg))) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        if (right_bdd_bytes != NULL)
            (*env)->ReleaseByteArrayElements(env, right_bdd_arr, right_bdd_bytes, 0);
        (*env)->ReleaseByteArrayElements(env, left_bdd_arr, left_bdd_bytes, 0);
        (*env)->ReleaseStringUTFChars(env, operator, operator_chars);

        (*env)->ThrowNew(env, error_class, (_errmsg ? _errmsg : "NULL"));

        return 0;
    }

    if (right_bdd_bytes != NULL)
        (*env)->ReleaseByteArrayElements(env, right_bdd_arr, right_bdd_bytes, 0);
    (*env)->ReleaseByteArrayElements(env, left_bdd_arr, left_bdd_bytes, 0);
    (*env)->ReleaseStringUTFChars(env, operator, operator_chars);

    jbyteArray ret = (*env)->NewByteArray(env, res_bdd->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, res_bdd->bytesize, (jbyte*)res_bdd);

    return ret;
}

JNIEXPORT jdouble JNICALL Java_com_doubtless_test_Test_bddProb
(JNIEnv* env, jobject obj, jbyteArray dict_arr, jbyteArray bdd_arr) {
    char* _errmsg;

    jbyte* dict_bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    jbyte* bdd_bytes = (*env)->GetByteArrayElements(env, bdd_arr, NULL);

    double prob = bdd_probability((bdd_dictionary*)dict_bytes, (bdd*)bdd_bytes, NULL, 0, &_errmsg);

    (*env)->ReleaseByteArrayElements(env, dict_arr, dict_bytes, 0);
    (*env)->ReleaseByteArrayElements(env, bdd_arr, bdd_bytes, 0);

    return (jdouble)prob;
}

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_test_Test_createDict
(JNIEnv* env, jobject obj, jstring vardefs) {
    bdd_dictionary new_dict_struct, *dict;
    bdd_dictionary* storage_dict = NULL;
    char* _errmsg = NULL;

    const char* vardefs_chars = (*env)->GetStringUTFChars(env, vardefs, 0);

    dict = bdd_dictionary_create(&new_dict_struct);

    if (!modify_dictionary(dict, DICT_ADD, (char*)vardefs_chars, &_errmsg)) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

        (*env)->ThrowNew(env, error_class, (_errmsg ? _errmsg : "NULL"));

        return 0;
    }

    storage_dict = dictionary_prepare2store(dict);

    (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

    jbyteArray ret = (*env)->NewByteArray(env, storage_dict->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, storage_dict->bytesize, (jbyte*)storage_dict);

    return ret;
}

JNIEXPORT jstring JNICALL Java_com_doubtless_test_Test_dict2string
(JNIEnv* env, jobject obj, jbyteArray dict_arr) {
    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);

    jbyte* bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    bdd_dictionary* dict = (bdd_dictionary*) bytes;

    bprintf(
        pbuff,
        "[Dictionary(#vars=%d, #values=%d)]",
        V_dict_var_size(dict->variables),
        V_dict_val_size(dict->values)-dict->val_deleted
    );
    jstring res = (*env)->NewStringUTF(env, pbuff->buffer);
    pbuff_free(pbuff);

    (*env)->ReleaseByteArrayElements(env, dict_arr, bytes, 0);

    return res;
}

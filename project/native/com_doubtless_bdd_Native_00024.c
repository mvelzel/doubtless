#include "com_doubtless_bdd_Native_00024.h"

#include "test_config.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "vector.h"
#include "utils.h"
#include "dictionary.h"
#include "bdd.h"

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_createBdd
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

    free(bdd);
    bdd = NULL;

    return ret;
}

JNIEXPORT jstring JNICALL Java_com_doubtless_bdd_Native_00024_bdd2string
(JNIEnv* env, jobject obj, jbyteArray bdd_arr) {
    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);

    jbyte* bytes = (*env)->GetByteArrayElements(env, bdd_arr, NULL);

    bdd* bdd_struct = (bdd*)bytes;
    (&bdd_struct->tree)->items = (&bdd_struct->tree)->fixed;

    bdd2string(pbuff, bdd_struct, 1);
    jstring res = (*env)->NewStringUTF(env, pbuff->buffer);
    pbuff_free(pbuff);

    (*env)->ReleaseByteArrayElements(env, bdd_arr, bytes, 0);

    return res;
}

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_bddOperator
(JNIEnv* env, jobject obj, jstring operator, jbyteArray left_bdd_arr, jbyteArray right_bdd_arr) {
    const char* operator_chars = (*env)->GetStringUTFChars(env, operator, 0);
    jbyte* left_bdd_bytes = (*env)->GetByteArrayElements(env, left_bdd_arr, NULL);
    jbyte* right_bdd_bytes = NULL;
    if (*operator_chars == '&' || *operator_chars == '|')
        right_bdd_bytes = (*env)->GetByteArrayElements(env, right_bdd_arr, NULL);
    char* _errmsg = NULL;

    bdd* left_bdd = (bdd*)left_bdd_bytes;
    (&left_bdd->tree)->items = (&left_bdd->tree)->fixed;

    bdd* right_bdd = (bdd*)right_bdd_bytes;
    if (right_bdd_bytes != NULL)
        (&right_bdd->tree)->items = (&right_bdd->tree)->fixed;
    
    bdd* res_bdd = NULL;
    if (!(res_bdd = bdd_operator(*operator_chars, BY_APPLY, left_bdd, right_bdd, &_errmsg))) {
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

    free(res_bdd);
    res_bdd = NULL;

    return ret;
}

JNIEXPORT jdouble JNICALL Java_com_doubtless_bdd_Native_00024_bddProb
(JNIEnv* env, jobject obj, jbyteArray dict_arr, jbyteArray bdd_arr) {
    char* _errmsg;

    jbyte* dict_bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);

    bdd_dictionary* dict = (bdd_dictionary*) dict_bytes;
    dict->variables = (V_dict_var*) &dict->buff[dict->var_offset];
    dict->values = (V_dict_val*) &dict->buff[dict->val_offset];

    jbyte* bdd_bytes = (*env)->GetByteArrayElements(env, bdd_arr, NULL);
    bdd* bdd_struct = (bdd*)bdd_bytes;
    (&bdd_struct->tree)->items = (&bdd_struct->tree)->fixed;

    double prob = bdd_probability(dict, bdd_struct, NULL, 0, &_errmsg);

    (*env)->ReleaseByteArrayElements(env, dict_arr, dict_bytes, 0);
    (*env)->ReleaseByteArrayElements(env, bdd_arr, bdd_bytes, 0);

    return (jdouble)prob;
}

JNIEXPORT jboolean JNICALL Java_com_doubtless_bdd_Native_00024_bddEqual
(JNIEnv* env, jobject obj, jbyteArray left_bdd_arr, jbyteArray right_bdd_arr) {
    char* _errmsg;

    jbyte* left_bdd_bytes = (*env)->GetByteArrayElements(env, left_bdd_arr, NULL);
    jbyte* right_bdd_bytes = (*env)->GetByteArrayElements(env, right_bdd_arr, NULL);

    bdd* left_bdd = (bdd*)left_bdd_bytes;
    (&left_bdd->tree)->items = (&left_bdd->tree)->fixed;

    bdd* right_bdd = (bdd*)right_bdd_bytes;
    (&right_bdd->tree)->items = (&right_bdd->tree)->fixed;

    jboolean equal = bdd_equal(left_bdd, right_bdd, &_errmsg);
    
    (*env)->ReleaseByteArrayElements(env, right_bdd_arr, right_bdd_bytes, 0);
    (*env)->ReleaseByteArrayElements(env, left_bdd_arr, left_bdd_bytes, 0);

    return equal;
}

JNIEXPORT jboolean JNICALL Java_com_doubtless_bdd_Native_00024_bddEquiv
(JNIEnv* env, jobject obj, jbyteArray left_bdd_arr, jbyteArray right_bdd_arr) {
    char* _errmsg;

    jbyte* left_bdd_bytes = (*env)->GetByteArrayElements(env, left_bdd_arr, NULL);
    jbyte* right_bdd_bytes = (*env)->GetByteArrayElements(env, right_bdd_arr, NULL);

    bdd* left_bdd = (bdd*)left_bdd_bytes;
    (&left_bdd->tree)->items = (&left_bdd->tree)->fixed;

    bdd* right_bdd = (bdd*)right_bdd_bytes;
    (&right_bdd->tree)->items = (&right_bdd->tree)->fixed;

    jboolean equal = bdd_equiv(left_bdd, right_bdd, &_errmsg);
    
    (*env)->ReleaseByteArrayElements(env, right_bdd_arr, right_bdd_bytes, 0);
    (*env)->ReleaseByteArrayElements(env, left_bdd_arr, left_bdd_bytes, 0);

    return equal;
}

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_createDict
(JNIEnv* env, jobject obj, jstring vardefs) {
    bdd_dictionary new_dict_struct, *dict;
    bdd_dictionary* storage_dict = NULL;
    char* _errmsg = NULL;

    const char* vardefs_chars = (*env)->GetStringUTFChars(env, vardefs, 0);

    dict = bdd_dictionary_create(&new_dict_struct);
    if (!dict) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

        (*env)->ThrowNew(env, error_class, "dictionary_in: dictionary create' failed");

        return 0;
    }

    if (!modify_dictionary(dict, DICT_ADD, (char*)vardefs_chars, &_errmsg)) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

        (*env)->ThrowNew(env, error_class, (_errmsg ? _errmsg : "NULL"));

        return 0;
    }

    storage_dict = dictionary_prepare2store(dict);
    if (!storage_dict) {
        jclass error_class = (*env)->FindClass(env, "java/lang/RuntimeException");

        (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

        (*env)->ThrowNew(env, error_class, "dictionary_add: internal error serialize/free/sort");

        return 0;
    }

    (*env)->ReleaseStringUTFChars(env, vardefs, vardefs_chars);

    jbyteArray ret = (*env)->NewByteArray(env, storage_dict->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, storage_dict->bytesize, (jbyte*)storage_dict);

    free(storage_dict);
    storage_dict = NULL;

    return ret;
}

JNIEXPORT jstring JNICALL Java_com_doubtless_bdd_Native_00024_dict2string
(JNIEnv* env, jobject obj, jbyteArray dict_arr) {
    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);

    jbyte* bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    bdd_dictionary* dict = (bdd_dictionary*) bytes;

    dict->variables = (V_dict_var*) &dict->buff[dict->var_offset];
    dict->values = (V_dict_val*) &dict->buff[dict->val_offset];

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

JNIEXPORT jstring JNICALL Java_com_doubtless_bdd_Native_00024_printDict
(JNIEnv* env, jobject obj, jbyteArray dict_arr) {
    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);

    jbyte* bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    bdd_dictionary* dict = (bdd_dictionary*) bytes;

    dict->variables = (V_dict_var*) &dict->buff[dict->var_offset];
    dict->values = (V_dict_val*) &dict->buff[dict->val_offset];

    bdd_dictionary_print(dict, 0, pbuff);
    jstring res = (*env)->NewStringUTF(env, pbuff->buffer);
    pbuff_free(pbuff);

    (*env)->ReleaseByteArrayElements(env, dict_arr, bytes, 0);

    return res;
}

JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_modifyDict
(JNIEnv* env, jobject obj, jbyteArray dict_arr, jint mode, jstring dict_def) {
    jbyte* dict_arr_bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    const char* dict_def_chars = (*env)->GetStringUTFChars(env, dict_def, 0);
    char* _errmsg = NULL;
    bdd_dictionary* storage_dict = NULL;

    bdd_dictionary* dict = (bdd_dictionary*) dict_arr_bytes;

    dict->variables = (V_dict_var*) &dict->buff[dict->var_offset];
    dict->values = (V_dict_val*) &dict->buff[dict->val_offset];

    if (!modify_dictionary(dict, (int)mode, (char*)dict_def_chars, &_errmsg)) {
        jclass error_class = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

        (*env)->ReleaseStringUTFChars(env, dict_def, dict_def_chars);
        (*env)->ReleaseByteArrayElements(env, dict_arr, dict_arr_bytes, 0);

        (*env)->ThrowNew(env, error_class, (_errmsg ? _errmsg : "NULL"));

        return 0;
    }

    storage_dict = dictionary_prepare2store(dict);
    if (!storage_dict) {
        jclass error_class = (*env)->FindClass(env, "java/lang/RuntimeException");

        (*env)->ReleaseStringUTFChars(env, dict_def, dict_def_chars);
        (*env)->ReleaseByteArrayElements(env, dict_arr, dict_arr_bytes, 0);

        (*env)->ThrowNew(env, error_class, "dictionary_add: internal error serialize/free/sort");

        return 0;
    }

    jbyteArray ret = (*env)->NewByteArray(env, storage_dict->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, storage_dict->bytesize, (jbyte*)storage_dict);

    (*env)->ReleaseStringUTFChars(env, dict_def, dict_def_chars);
    (*env)->ReleaseByteArrayElements(env, dict_arr, dict_arr_bytes, 0);

    free(storage_dict);
    storage_dict = NULL;

    return ret;
}

JNIEXPORT jobjectArray JNICALL Java_com_doubtless_bdd_Native_00024_getKeys
(JNIEnv* env, jobject obj, jbyteArray dict_arr) {
    jbyte* dict_arr_bytes = (*env)->GetByteArrayElements(env, dict_arr, NULL);
    bdd_dictionary* dict = (bdd_dictionary*) dict_arr_bytes;

    dict->variables = (V_dict_var*) &dict->buff[dict->var_offset];
    dict->values = (V_dict_val*) &dict->buff[dict->val_offset];

    jobjectArray ret = (*env)->NewObjectArray(
        env,
        V_dict_val_size(dict->values) - dict->val_deleted,
        (*env)->FindClass(env, "java/lang/String"),
        (*env)->NewStringUTF(env, "")
     );

    int k = 0;
    for (int i = 0; i < V_dict_var_size(dict->variables); i++) {
        dict_var* var = V_dict_var_getp(dict->variables, i);
        for (dindex j = var->offset; j < (var->offset + var->card); j++) {
            dict_val* val = V_dict_val_getp(dict->values, j);
            int length = snprintf(NULL, 0, "%s=%d", var->name, val->value);
            char* str = malloc(length + 1);
            snprintf(str, length + 1, "%s=%d", var->name, val->value);

            (*env)->SetObjectArrayElement(env, ret, k, (*env)->NewStringUTF(env, str));

            free(str);
            str = NULL;
            k++;
        }
    }

    (*env)->ReleaseByteArrayElements(env, dict_arr, dict_arr_bytes, 0);

    return ret;
}

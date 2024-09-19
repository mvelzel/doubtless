#include "Test.h"

#include "test_config.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "vector.h"
#include "utils.h"
#include "dictionary.h"
#include "bdd.h"

JNIEXPORT jbyteArray JNICALL Java_Test_createBdd
(JNIEnv* env, jobject obj, jstring expr) {
    const char* expr_chars = (*env)->GetStringUTFChars(env, expr, 0);
    char* _errmsg = NULL;

    bdd* bdd = create_bdd(BDD_DEFAULT, (char*)expr_chars, &_errmsg, 0);

    (*env)->ReleaseStringUTFChars(env, expr, expr_chars);

    jbyteArray ret = (*env)->NewByteArray(env, bdd->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, bdd->bytesize, (jbyte*)bdd);
    return ret;
}

JNIEXPORT jstring JNICALL Java_Test_bdd2string
(JNIEnv* env, jobject obj, jbyteArray bddArr) {
    jbyte* bytes = (*env)->GetByteArrayElements(env, bddArr, NULL);

    pbuff pbuff_struct, *pbuff=pbuff_init(&pbuff_struct);
    bdd2string(pbuff, (bdd*)bytes, 1);
    jstring res = (*env)->NewStringUTF(env, pbuff->buffer);
    pbuff_free(pbuff);

    (*env)->ReleaseByteArrayElements(env, bddArr, bytes, 0);

    return res;
}

JNIEXPORT jbyteArray JNICALL Java_Test_bddOperator
(JNIEnv* env, jobject obj, jstring operator, jbyteArray leftBddArr, jbyteArray rightBddArr) {
    const char* operator_chars = (*env)->GetStringUTFChars(env, operator, 0);
    jbyte* leftBddBytes = (*env)->GetByteArrayElements(env, leftBddArr, NULL);
    jbyte* rightBddBytes = NULL;
    if (*operator_chars == '&' || *operator_chars == '|')
        rightBddBytes = (*env)->GetByteArrayElements(env, rightBddArr, NULL);
    char* _errmsg = NULL;
    
    bdd* resBdd = bdd_operator(*operator_chars, BY_APPLY, (bdd*)leftBddBytes, (bdd*)rightBddBytes, &_errmsg);

    if (rightBddBytes != NULL)
        (*env)->ReleaseByteArrayElements(env, rightBddArr, rightBddBytes, 0);
    (*env)->ReleaseByteArrayElements(env, leftBddArr, leftBddBytes, 0);
    (*env)->ReleaseStringUTFChars(env, operator, operator_chars);

    jbyteArray ret = (*env)->NewByteArray(env, resBdd->bytesize);
    (*env)->SetByteArrayRegion(env, ret, 0, resBdd->bytesize, (jbyte*)resBdd);

    return ret;
}

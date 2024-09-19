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

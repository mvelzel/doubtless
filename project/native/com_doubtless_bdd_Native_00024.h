/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_doubtless_bdd_Native_00024 */

#ifndef _Included_com_doubtless_bdd_Native_00024
#define _Included_com_doubtless_bdd_Native_00024
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     createBdd
 * Signature:  (Ljava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_createBdd
  (JNIEnv *, jobject, jstring);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     bdd2string
 * Signature:  ([B)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_doubtless_bdd_Native_00024_bdd2string
  (JNIEnv *, jobject, jbyteArray);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     bddOperator
 * Signature:  (Ljava/lang/String;[B[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_bddOperator
  (JNIEnv *, jobject, jstring, jbyteArray, jbyteArray);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     bddProb
 * Signature:  ([B[B)D
 */
JNIEXPORT jdouble JNICALL Java_com_doubtless_bdd_Native_00024_bddProb
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     bddEqual
 * Signature:  ([B[B)Z
 */
JNIEXPORT jboolean JNICALL Java_com_doubtless_bdd_Native_00024_bddEqual
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     createDict
 * Signature:  (Ljava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_doubtless_bdd_Native_00024_createDict
  (JNIEnv *, jobject, jstring);

/*
 * Class:      com_doubtless_bdd_Native_00024
 * Method:     dict2string
 * Signature:  ([B)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_doubtless_bdd_Native_00024_dict2string
  (JNIEnv *, jobject, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif

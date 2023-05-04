#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

struct Bucket {
    float value;
    struct Bucket* next;
    int count;
};

struct BucketsortData {
    float** array;
    int start;
    int end;
    int extraThreads;
    int* sizes;
    struct Bucket* buckets;
    pthread_barrier_t syncBarrier;
};

struct MergesortData {
    float* array;
    int start;
    int end;
    pthread_barrier_t syncBarrier;
};

/*
 * The majority of the sequential code in this project is replicated across..-
 * -.. all versions of this algorithm - MPI, Pthreads and OpenMP.
 */
int sort_buckets(struct BucketsortData data);
void initialiseBuckets(struct Bucket* buckets, int bucketCount);
int fillBuckets(const float* floatArrayToSort, int size,
                struct Bucket* buckets, int bucketCount);
void freeBuckets(struct Bucket* buckets, int bucketCount);
void mergesort_manager(struct MergesortData data);
int mergesort_parallel(float* floatArrayToSort, int size, int threadCount);
void mergesort(float* array, int low, int high);
void merge(float* floatArrayToSort, int low, int mid, int high);

int bucketsort(float* floatArrayToSort, int arraySize, int threadCount,
               int bucketCount, int threadsPerThread) {
    /*
     * @param floatArrayToSort      Array to sort, self-descriptive
     * @param arraySize             Size of the array to sort
     * @param bucketCount           Number of buckets
     * @param threadsPerThread      Number of threads per thread to perform ..-
     *                              -.. parallel mergesort. 1 = sequential.
     */
    if (bucketCount < 1 || threadCount < 1 || bucketCount > arraySize) {
        printf("Invalid input.\n");
        return 0;
    }
    else if (bucketCount == 1) { // Sequential
        return mergesort_parallel(floatArrayToSort, arraySize, threadsPerThread);
    }

    if (threadCount > bucketCount) threadCount = bucketCount;

    struct Bucket* buckets = (struct Bucket*) malloc((size_t) bucketCount * sizeof(struct Bucket));
    if (!buckets) {
        printf("An error has occurred.\n");
        return 0;
    }

    initialiseBuckets(buckets, bucketCount);
    // negative numbers in list or failure to malloc
    if (!fillBuckets(floatArrayToSort, arraySize, buckets, bucketCount)) {
        printf("An error has occurred.\n");
        return 0;
    }

    float** numbersInBuckets;
    numbersInBuckets = (float**) malloc(bucketCount * sizeof(float*));
    if (!numbersInBuckets) {
        printf("An error has occurred.\n");
        freeBuckets(buckets, bucketCount);
        return 0;
    }

    pthread_t* threads = (pthread_t*) malloc(threadCount * sizeof(pthread_t));
    if (!threads) {
        printf("An error has occurred.\n");
        free(numbersInBuckets);
        freeBuckets(buckets, bucketCount);
        return 0;
    }

    struct BucketsortData data;
    if (pthread_barrier_init(&data.syncBarrier, NULL, 2)) {
        printf("An error has occurred.\n");
        freeBuckets(buckets, bucketCount);
        free(numbersInBuckets);
        free(threads);
        return 0;
    }

    int sizes[bucketCount];


    int portion, remainder;
    portion = bucketCount / threadCount;
    remainder = bucketCount % threadCount;

    data.array = numbersInBuckets;
    data.buckets = buckets;
    data.sizes = sizes;
    data.extraThreads = threadsPerThread;
    for (int i = 0; i < threadCount; i++) {
        // Sort buckets in parallel
        if (i < remainder) {
            data.start = i * (portion + 1);
            data.end = data.start + portion + 1;
        }
        else {
            data.start = portion*(i - remainder) + remainder*(portion + 1);
            data.end = data.start + portion;
        }
        if (pthread_create(&threads[i], NULL, (void*) sort_buckets,&data)) {
            printf("An error has occurred.\n");
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            for (int j = 0; j < bucketCount; j++) free(numbersInBuckets[j]);
            free(numbersInBuckets);
            freeBuckets(buckets, bucketCount);
            free(threads);
            pthread_barrier_destroy(&data.syncBarrier);
            return 0;
        }
        pthread_barrier_wait(&data.syncBarrier);
    }
    pthread_barrier_destroy(&data.syncBarrier);

    int returnVal;
    int errorOccurred = 0;
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], (void**) &returnVal);
        if (!returnVal) {
            printf("An error has occurred.\n");
            errorOccurred = 1;
        }
    }

    free(buckets);
    free(threads);
    if (!errorOccurred) {
        int k = 0;
        for (int i = 0; i < bucketCount; i++) {
            for (int j = 0; j < sizes[i]; j++) {
                floatArrayToSort[k] = numbersInBuckets[i][j];
                k++;
            }
            free(numbersInBuckets[i]);
        }
        free(numbersInBuckets);
        return 1;
    }
    free(numbersInBuckets);
    return 0;
}

void initialiseBuckets(struct Bucket* buckets, int bucketCount) {
    for (int i = 0; i < bucketCount; i++) {
        buckets[i].value = -1;
        buckets[i].next = NULL;
        buckets[i].count = 0;
    }
}

void freeBuckets(struct Bucket* buckets, int bucketCount) {
    struct Bucket *prevBucket, *currentBucket;
    for (int i = 0; i < bucketCount; i++) {
        currentBucket = &buckets[i];
        for (int j = 0; j < currentBucket->count; j++) {
            prevBucket = currentBucket;
            currentBucket = currentBucket->next;
            if (j == 0) continue; // Ignore first bucket allocated on stack
            free(prevBucket);
        }
    }
    free(buckets);
}

int fillBuckets(const float* floatArrayToSort, int size, struct Bucket* buckets, int bucketCount) {
    float currentItem;
    struct Bucket *bucket;
    for (int i = 0; i < size; i++) {
        currentItem = floatArrayToSort[i];
        if (currentItem < 0) {
            freeBuckets(buckets, bucketCount);
            printf("Invalid input: Negative numbers.\n");
            return 0; // No negative numbers allowed
        }
        // Assumes the majority of numbers are between 0 and 1.
        if (currentItem < (float) (bucketCount - 1) / (float) bucketCount) {
            bucket = &(buckets[(int) (currentItem * (float) bucketCount)]);
        } else { // Store in final bucket
            bucket = &buckets[bucketCount-1];
        }
        bucket->count++;
        if ((int) bucket->value == -1) {
            bucket->value = currentItem;
            continue;
        }

        // Insert new bucket
        struct Bucket *newBucket = (struct Bucket *)
                malloc(sizeof(struct Bucket));
        if (newBucket == NULL) {
            freeBuckets(buckets, bucketCount);
            return 0;
        }
        newBucket->next = bucket->next;
        bucket->next = newBucket;

        newBucket->value = currentItem;
        newBucket->count = 1;
    }
    return 1;
}

int sort_buckets(struct BucketsortData data) {
    int start = data.start;
    int end = data.end;
    pthread_barrier_wait(&data.syncBarrier);
    int* sizes = data.sizes;
    struct Bucket* buckets = data.buckets;
    float** numbersInBuckets = data.array;

    /* Gather items in each bucket and store in a separate array */
    int itemsInBucket;
    struct Bucket* currentBucket;
    struct Bucket* prevBucket;
    for (int i = start; i < end; i++) {
        currentBucket = &buckets[i];
        itemsInBucket = currentBucket->count;
        sizes[i] = itemsInBucket;
        numbersInBuckets[i] = (float*) malloc(itemsInBucket * sizeof(float));
        if (!numbersInBuckets[i]) {
            for (int j = start; j < i; j++) free(numbersInBuckets[j]);
            return 0;
        }
        if (!itemsInBucket) {
            continue;
        }
        for (int j = 0; j < itemsInBucket; j++) {
            numbersInBuckets[i][j] = currentBucket->value;
            prevBucket = currentBucket;
            currentBucket = currentBucket->next;
            if (j == 0) continue; // First bucket allocated on stack
            free(prevBucket);
        }
        // Sort bucket
        if (!mergesort_parallel(numbersInBuckets[i],
                                    itemsInBucket, data.extraThreads)) {
            return 0;
        }
    }
    return 1;
}

int mergesort_parallel(float* floatArrayToSort, int size, int threadCount) {
    if (threadCount < 2) { // Sequential
        mergesort(floatArrayToSort, 0, size - 1);
        return 1;
    }
    else if (threadCount > size)  threadCount = size; // Too many threads

    pthread_t* threads = (pthread_t*) malloc(threadCount * sizeof(pthread_t));
    if (!threads) {
        return 0;
    }

    struct MergesortData data;
    if (pthread_barrier_init(&data.syncBarrier, NULL, 2)) {
        free(threads);
        return 0;
    }

    data.array = floatArrayToSort;
    int portion = size / threadCount;
    int remainder = size % threadCount;
    int starts[threadCount];
    int portions[threadCount];
    for (int i = 0; i < threadCount; i++) {
        // Perform mergesort on array
        if (i < remainder) {
            data.start = i * (portion + 1);
            data.end = data.start + portion + 1;
            portions[i] = portion + 1;
        }
        else {
            data.start = portion*(i - remainder) + remainder*(portion + 1);
            data.end = data.start + portion;
            portions[i] = portion;
        }
        starts[i] = data.start;
        if (pthread_create(&threads[i], NULL, (void*) mergesort_manager,&data)) {
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            pthread_barrier_destroy(&data.syncBarrier);
            free(threads);
            return 0;
        }
        pthread_barrier_wait(&data.syncBarrier);
    }
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }
    pthread_barrier_destroy(&data.syncBarrier);
    free(threads);
    /* Final merges */
    int low, mid, high, temp;
    low = 0;
    high = portions[0]-1;
    for (int i = 0; i < threadCount-1; i++) {
        temp = portions[i+1]-1 + starts[i+1];
        mid = high;
        high = temp;
        merge(floatArrayToSort, low, mid, high);
    }
    return 1;
}

void mergesort_manager(struct MergesortData data) {
    // Perform mergesort on unique range
    int start = data.start;
    int end = data.end;
    pthread_barrier_wait(&data.syncBarrier);
    mergesort(data.array, start, end-1);
}

void mergesort(float* array, int low, int high) {
    // Ordinary mergesort
    if (low >= high) return;
    int mid = low + (high - low)/2;
    mergesort(array, low, mid);
    mergesort(array, mid + 1, high);
    merge(array, low, mid, high);
}

void merge(float* floatArrayToSort, int low, int mid, int high) {
    int i, j, k;
    int lengthOfA = mid - low + 1;
    int lengthOfB = high - mid;
    float *a, *b;
    a = malloc(lengthOfA * sizeof(float));
    if (a) {
        b = malloc(lengthOfB * sizeof(float));
        if (!b) {
            free(a);
            printf("An error has occurred.\n");
            return;
        }
    }
    else {
        printf("An error has occurred.\n");
        return;
    }

    for (i = 0; i < lengthOfA; i++) {
        a[i] = floatArrayToSort[i + low];
    }
    for (j = 0; j < lengthOfB; j++) {
        b[j] = floatArrayToSort[j + mid + 1];
    }
    i = j = 0;
    k = low;
    while (i < lengthOfA && j < lengthOfB) {
        if (a[i] <= b[j]) {
            floatArrayToSort[k] = a[i];
            i++;
        }
        else {
            floatArrayToSort[k] = b[j];
            j++;
        }
        k++;
    }
    for (;i < lengthOfA; i++) {
        floatArrayToSort[k] = a[i];
        k++;
    }
    for (;j < lengthOfB; j++) {
        floatArrayToSort[k] = b[j];
        k++;
    }
    free(a); free(b);
}

int main() {
    int size = 10000000;
    float *array = (float *) malloc((size_t) size * sizeof(float));
    if (array == NULL) return -1;
    time_t t;
    srand((unsigned) time(&t));
    for (int i = 0; i < size; i++) {
        array[i] = (float) rand() / (float) RAND_MAX;
    }
    int result;
    clock_t start, end;
    start = clock();
    result = bucketsort(array, size, 8, 20, 1);
    end = clock();
    if (!result) {
        free(array);
        return 0;
    }
    int incorrectCounter, correctCounter;
    incorrectCounter = correctCounter = 0;
    for (int i = 1; i < size; i++) {
        if (array[i] < array[i - 1]) incorrectCounter++;
        else correctCounter++;
    }
    correctCounter++; // final unaccounted number
    printf("Sorted numbers: %d\nIncorrectly sorted numbers: %d\nTotal numbers: %d\n",
           correctCounter, incorrectCounter, size);
    printf("Time taken: %g seconds\n", (double)(end-start) / CLOCKS_PER_SEC);
    FILE *f = fopen("times.txt", "a");
    fprintf(f, "%g,", (double)(end-start) / CLOCKS_PER_SEC);
    free(array);
    return 0;
}
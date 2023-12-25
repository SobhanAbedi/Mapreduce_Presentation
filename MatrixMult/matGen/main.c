#include <stdio.h>
#include <stdlib.h>

#define M 16

int main(int argc, char** argv) {
	if (argc < 4) {
		printf("Correct Usage: matGent <M> <N> <P> [seed] (will output two matrices. one MxN another NxP)\n");
		return -1;
	}
	int m,n,p;
	int seed = 0;
	sscanf(argv[1], "%d", &m);
	sscanf(argv[2], "%d", &n);
	sscanf(argv[3], "%d", &p);
	if (argc > 4)
		sscanf(argv[4], "%d", &seed);
	srand(seed);
	int a = m;
	int b = n;
	for (int k = 0; k < 2; k++) {
		for (int i = 0; i < a; i++)
			for (int j = 0; j < b; j++)
				printf("%d,%d,%d,%d\n", k, i, j, rand() % M);
		a = n;
		b = p;
	}
	return 0;
}

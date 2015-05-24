.PHONY: test

all: test clean

test: clean
	python2 omlite_test.py
	python3 omlite_test.py

clean:
	git clean -df

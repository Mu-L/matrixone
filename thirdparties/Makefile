# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PWD=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
UNAME_S=$(shell uname -s | tr A-Z a-z)
UNAME_M=$(shell uname -m)
# Usearch source code is from https://github.com/cpegeric/usearch/tree/bug564
# There is a fix for concurrent search issue.
USEARCH_DIR=usearch-2.18.0
USEARCH_TAR=$(USEARCH_DIR).tar.gz
STRINGZILLA_DIR=StringZilla-3.12.5
STRINGZILLA_TAR=$(STRINGZILLA_DIR).tar.gz
SIMSIMD_DIR=SimSIMD-6.5.0
SIMSIMD_TAR=$(SIMSIMD_DIR).tar.gz
FP16_DIR=fp16
FP16_TAR=$(FP16_DIR).tar.gz

all: init usearch

init:
	mkdir -p install
	mkdir -p install/include install/lib

usearch: stringzilla fp16 simsimd install/include/usearch.h

stringzilla: install/include/stringzilla/stringzilla.h

simsimd: install/include/simsimd/simsimd.h

fp16: install/include/fp16.h

simsimd: install/include/simsimd/simsimd.h

install/include/fp16.h:
	tar zxvf $(FP16_TAR)
	cp -r $(FP16_DIR)/include/* install/include

install/include/simsimd/simsimd.h:
	tar zxvf $(SIMSIMD_TAR)
	cp -r $(SIMSIMD_DIR)/include/* install/include

install/include/stringzilla/stringzilla.h:
	tar zxvf $(STRINGZILLA_TAR)
	cp -r $(STRINGZILLA_DIR)/include/* install/include

USEARCH_CMAKE_FLAG := -DUSEARCH_BUILD_BENCH_CPP=OFF -DUSEARCH_BUILD_TEST_C=OFF -DUSEARCH_BUILD_TEST_CPP=OFF -DUSEARCH_BUILD_LIB_C=ON -DCMAKE_CXX_FLAGS=-I$(PWD)/install/include
AVX512FP16 :=

ifeq ($(UNAME_S),linux)
  ifeq ($(MUSL),)
	USEARCH_CMAKE_FLAG += -DUSEARCH_USE_OPENMP=1
  endif
  AVX512FP16 := $(shell lscpu | grep avx512fp16)
endif

ifeq ($(UNAME_M),x86_64)
  ifneq ($(AVX512FP16),)
    ifeq ($(MUSL),)
	USEARCH_CMAKE_FLAG += -DUSEARCH_USE_SIMSIMD=1
    endif
  endif
endif

install/include/usearch.h:
	rm -rf $(USEARCH_DIR)
	tar zxvf $(USEARCH_TAR)
	cp -r $(FP16_DIR)/* $(USEARCH_DIR)/fp16
	cp -r $(SIMSIMD_DIR)/* $(USEARCH_DIR)/simsimd
	cp -r $(STRINGZILLA_DIR)/* $(USEARCH_DIR)/stringzilla
	(cd $(USEARCH_DIR) && cmake -B build $(USEARCH_CMAKE_FLAG) && cmake --build build --config Release)
ifeq ($(UNAME_S),darwin)
	cp $(USEARCH_DIR)/build/*.dylib install/lib
endif
ifeq ($(UNAME_S),linux)
	cp $(USEARCH_DIR)/build/*.so install/lib
endif
	cp $(USEARCH_DIR)/build/libusearch_static_c.a install/lib/libusearch_c.a
	cp $(USEARCH_DIR)/c/usearch.h install/include
	rm -rf $(FP16_DIR)
	rm -rf $(SIMSIMD_DIR)
	rm -rf $(STRINGZILLA_DIR)
	rm -rf $(USEARCH_DIR)

clean:
	rm -rf install $(USEARCH_DIR) $(STRINGZILLA_DIR) $(SIMSIMD_DIR) $(FP16_DIR)

.PHONY: usearch stringzilla fp16 simsimd init

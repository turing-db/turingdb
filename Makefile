BUILD_ENV=
BUILD_DIR=$(abspath build_package)
SRC_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

all: build

.PHONY: build
build: JOBS=$(filter -j%,$(MAKEFLAGS))
build:
	@mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) \
	&& date +%s > .build_start \
	&& $(BUILD_ENV) cmake -DCMAKE_INSTALL_PREFIX=$(TURING_HOME) $(SRC_DIR) \
	&& make -s $(JOBS) \
	&& make install \
	&& date +%s > .build_end \
	&& echo $$(expr $$(cat .build_end) - $$(cat .build_start)) > .build_time \
	&& echo "Build passed in $$(cat .build_time) seconds."

.PHONY: debug
debug: BUILD_ENV += DEBUG_BUILD=1
debug: build

.PHONY: reldebug
reldebug: BUILD_ENV += RELDEBUG=1
reldebug: build

.PHONY: callgrind
callgrind: BUILD_ENV += CALLGRIND_PROFILE=1
callgrind: build

.PHONY: clean
clean:

check-env:
ifndef TURING_HOME
	$(error TURING_HOME is not defined, please configure your environment.)
endif

.PHONY: test
test:
	cd $(BUILD_DIR) && ctest --output-on-failure --output-junit ../unit_tests.xml --force-new-ctest-process
regress:
	cd $(SRC_DIR)/test/regress && wrt -output-on-failure -j 4
run_samples:
	cd $(TURING_HOME)/samples && wrt -output-on-failure -j 4

#!/bin/bash

prod_ready_bins=biorun

package_name=turing
turing_img=799170964164.dkr.ecr.eu-west-2.amazonaws.com/turing:latest
turing_dir=$1

function build_package() {
	# Build turing package
	docker run -it --user root -v $turing_dir:/home/dev/turing $turing_img /home/dev/turing/release/scripts/build_package.sh
	error=$?

	if [ ! "$error" == "0" ] ; then
		echo 'Build failed'
		docker run -it -v $turing_dir:/home/dev/turing $turing_img bash
		exit 1
	fi

	# Create final package directory
	rm -rf $package_name
	mkdir -p $package_name/bin
	for bin_file in "$prod_ready_bins"
	do
		echo "Copying $bin_file binary into release package"
		cp build/turing_package/bin/$bin_file $package_name/bin
	done

	cp -r build/turing_package/lib $package_name/lib
	rm -rf $package_name/lib/python/turing/db

	cp -r build/turing_package/notebooks $package_name/notebooks

	# Create package archive
	tar -czvf $package_name.tar.gz $package_name
}

function clean_build() {
    rm -rf build
    rm -rf turing
}

# Check turing_dir
if [ ! -d "$turing_dir" ] ; then
    echo 'ERROR: please give the path to the turing source directory'
    exit 1
fi

$(aws ecr get-login --region eu-west-2 --no-include-email)

build_package

# Build delivery docker image
echo 'Building turing-platform image'
docker build -t turing-platform-sanofi:latest --build-arg BASE_IMAGE=$turing_img --build-arg TURING_PACKAGE=$package_name .
docker tag turing-platform-sanofi:latest 799170964164.dkr.ecr.eu-west-2.amazonaws.com/turing-platform-sanofi:latest
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 799170964164.dkr.ecr.eu-west-2.amazonaws.com
docker push 799170964164.dkr.ecr.eu-west-2.amazonaws.com/turing-platform-sanofi:latest

# Clean build files
clean_build

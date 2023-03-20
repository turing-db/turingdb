#!/bin/bash

if [ ! -d "$1" ] ; then
    echo 'ERROR: ./generate_archive.sh /path/to/site /path/to/output_dir'
    exit 1
fi

if [ ! -d  "$2" ] ; then
    echo 'ERROR: ./generate_archive.sh /path/to/site /path/to/output_dir'
    exit 1
fi

site_dir=$1
output_dir=$2
archive_path=$output_dir/site.tar
base_site_dir=$(dirname $site_dir)
relative_site_dir=$(basename $site_dir)

cd $site_dir && ./build.sh

#cd $site_dir && ./clean.sh
cd $base_site_dir && tar -cf $archive_path $relative_site_dir > /dev/null
echo "
#ifndef _UI_SITE_ARCHIVE_DATA_
#define _UI_SITE_ARCHIVE_DATA_

#define INCBIN_PREFIX globfs_
#define INCBIN_STYLE INCBIN_STYLE_SNAKE
#include \"incbin.h\"

INCBIN(site, \"$archive_path\");

#endif
" >> $output_dir/SiteArchiveData.h

#! /bin/bash
set -ex
go build -v
tar -czvf journalbeat.tar.gz journalbeat
fpm -s tar -t rpm -n journalbeat -v 5.1.2 journalbeat.tar.gz
fpm -s tar -t deb -n journalbeat -v 5.1.2 journalbeat.tar.gz
mv journalbeat*.rpm build/
mv journalbeat*.deb build/
mv journalbeat build/
rm journalbeat.tar.gz
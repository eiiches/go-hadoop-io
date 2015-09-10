go-hadoop-io
============

Hadoop SequenceFile reader for Go. This project is still in early stage and may be subject to change.

Installation
------------

Ubuntu
```sh
sudo apt-get install libbz2-dev liblz4-dev
go get github.com/eiiches/go-hadoop-io
```

CentOS
```sh
sudo yum install bzip2-devel
git clone https://github.com/Cyan4973/lz4
cd lz4
make
sudo make install
sudo ldconfig
go get github.com/eiiches/go-hadoop-io
```

Usage
-----

Some examples are located in [examples](examples).

License
-------

This project is licensed under the terms of the 3-clause BSD license. See [LICENSE.txt](LICENSE.txt) for the full license text.

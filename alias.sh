alias dc="docker-compose"
dc-rs() {
    dc stop $1; dc rm -f $1; dc build $1; dc up -d $1; dc logs -f $1
}
dc-rm() {
    dc stop $1; dc rm -f $1
}

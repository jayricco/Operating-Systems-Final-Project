#/bin/bash
printf "Copying data file so as to not ruin the original...\n"
cp -f -v ./data_128_master data_128
printf "Compiling the program to follow the latest update...\n"
gcc -o mtfs mtfs.c -lpthread
read -s -t 2 -p "Ready to run? (y/n): " -n 1 response
printf "\n"
if [[ $response == 'y' || $response == 'Y' ]]; then
    ./mtfs data_128
fi

#/bin/bash
printf "Copying data file so as to not ruin the original...\n"
cp -f -v ./data_512k_master data_512k
printf "Compiling the program to follow the latest update...\n"
gcc -o mtfs mtfs.c -lpthread
read -s -t 2 -p "Ready to run? (y/n): " -n 1 response
printf "\n"

if [[ $response == 'y' || $response == 'Y' ]]; then
    for num_threads in 1 2 4 8 16
    do
        if [[ $num_threads == 1 ]]; then
            ./mtfs -t 10 -n $num_threads data_512k
        else
            ./mtfs -t 10 -n $num_threads data_512k
        fi
    done
fi

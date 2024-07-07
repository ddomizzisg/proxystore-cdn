
for m in 5 10 20 40 80 160
do 
bash  stop_containers.sh
bash deploy_new_containers.sh $m
for i in 1 2 4 8 16 32
do
    for j in 0 1
    do
        for k in 0 # 1
        do
            python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/random/1M  --repeat=10 --clients $i  --resiliency $j --parallel $k  --csv-file varying${m}_res_${j}_parallel_${k}.csv
	    
	    curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655

            python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/random/10M  --repeat=10 --clients $i  --resiliency $j --parallel $k  --csv-file varying${m}_res_${j}_parallel_${k}.csv
	    
	    curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655

            python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/random/100M  --repeat=10 --clients $i  --resiliency $j --parallel $k  --csv-file varying${m}_res_${j}_parallel_${k}.csv

	    curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655

            python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/random/1000M  --repeat=10 --clients $i  --resiliency $j --parallel $k  --csv-file varying${m}_res_${j}_parallel_${k}.csv

	    curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655
        done
    done
done
done

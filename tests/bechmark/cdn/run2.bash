python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/random/${1}M  --repeat=1 --clients 1  --resiliency 0 --parallel 1 

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/random/${1}M  --repeat=3 --clients 1  --resiliency 1 --parallel 1  --csv-file aws_lustre_nozip_${2}res.cs

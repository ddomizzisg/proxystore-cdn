python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog 322ad9917fa9692c68d512afad44ce3f1199b422c183fa4fa020a77f75eeaf91  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --clients 24  --csv-file pruebas2mexico.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee -a loadbalancing.txt

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-user
token 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --cl
ients 24  --csv-file pruebas2mexicodownload.csv

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-user
token 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --cl
ients 12  --csv-file pruebas2mexicodownload.csv

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-user
token 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --cl
ients 6  --csv-file pruebas2mexicodownload.csv

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-user
token 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --cl
ients 3  --csv-file pruebas2mexicodownload.csv

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog ddbb0616acf60b098cfb34e6bcbdd3970bf1583fdb11320dc60243529185e502  --cdn-user
token 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops GET --files /DATA/dsanchez/medicaldata/tomographies/  --repeat=1 --cl
ients 1  --csv-file pruebas2mexicodownload.csv


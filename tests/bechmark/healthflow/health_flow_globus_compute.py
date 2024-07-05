from __future__ import annotations



import argparse
import sys
import time
from typing import Any
import glob
import pydicom
import os

import numpy as np
from globus_compute_sdk import Executor
from globus_compute_sdk import Client

from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.redis import RedisConnector
from proxystore.store import register_store
from proxystore.store.base import Store
from proxystore.connectors.cdn import CDNConnector

import matplotlib.pylab as plt
#from PIL import Image

# First, define the function ...
def add_func(a, b):
    return a + b

def dicom2png(dicom, path):
    import matplotlib.pylab as plt
    from PIL import Image
    import pydicom
    import numpy as np
    import os
    import sys

    
    # Get the pixel array from the DICOM file
    pixel_array = dicom.pixel_array
    output_image_path = os.path.basename(path) + ".png"
    # Convert the pixel array to a PIL image
    plt.imsave(output_image_path, pixel_array, cmap='gray')
    plt.close()
    # El canal A (alpha) es incompatible con tensorflow, es necesario volver a guardar la imagen en RGB
    I = Image.open(output_image_path)
    J = I.convert('RGB')
    #J.save(output_image_path)
    return J

def detector(image: np.array):
    """Detects objects in image."""
    import sys
    import os
    
    
    # Add the path to the sys.path
    sys.path.append("/code")
    sys.path.append("/code/models/research/")

    
    import detectorHuesos as dh
    return dh.detectar(image)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='HealthFlow with Globus Compute and ProxyStore',
    )
    parser.add_argument(
        '-e',
        '--endpoint',
        required=True,
        help='Globus Compute endpoint for task execution',
    )
    
    parser.add_argument(
        '-d',
        '--dir',
        required=True,
        help='Directory to store proxied object in.',
    )
    
    ps_group = parser.add_argument_group()
    ps_backend_group = ps_group.add_mutually_exclusive_group(required=False)
    ps_backend_group.add_argument(
        '--ps-file',
        action='store_true',
        help='Use the ProxyStore file backend.',
    )
    ps_backend_group.add_argument(
        '--ps-globus',
        action='store_true',
        help='Use the ProxyStore Globus backend.',
    )
    ps_backend_group.add_argument(
        '--ps-redis',
        action='store_true',
        help='Use the ProxyStore Redis backend.',
    )
    ps_backend_group.add_argument(
        '--ps-cdn',
        action='store_true',
        help='Use the ProxyStore CDN backend.',
    )
    ps_group.add_argument(
        '--ps-file-dir',
        required='--ps-file' in sys.argv,
        help='Temp directory to store proxied object in.',
    )
    ps_group.add_argument(
        '--ps-globus-config',
        required='--ps-globus' in sys.argv,
        help='Globus Endpoint config file to use with ProxyStore.',
    )
    ps_group.add_argument(
        '--ps-redis-port',
        type=int,
        required='--ps-redis' in sys.argv,
        help='Redis server running on the localhost to use with ProxyStore',
    )
    args = parser.parse_args()

    store: Store[Any] | None = None
    if args.ps_file:
        store = Store('file', FileConnector(store_dir=args.ps_file_dir))
    elif args.ps_globus:
        endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
        store = Store('globus', GlobusConnector(endpoints=endpoints))
    elif args.ps_redis:
        store = Store('redis', RedisConnector('129.114.26.127', args.ps_redis_port))
    elif args.ps_cdn:
        store = Store('cdn', CDNConnector(catalog="proxystore"))
    
    start = time.perf_counter()
    
    with Executor(endpoint_id=args.endpoint) as gce:
        path = args.dir
        
        #List images in the directory
        images = glob.glob(f'{path}/*.dcm')
        futures = []
        for img in images:
            dicom = pydicom.dcmread(img)
            if store is not None:
                dicom = store.proxy(dicom)
            #print(dicom)
            futures.append(gce.submit(dicom2png, dicom, img))
        
        png_results = [future.result() for future in futures]
        
        futures = []
        for png in png_results:
            if store is not None:
                png = store.proxy(png)
            #print(png)
            futures.append(gce.submit(detector, png))
        
        detector_results = [future.result() for future in futures]
        
        for i,d in enumerate(detector_results):
            #print(d)
            plt.figure(figsize=(15,10))
            plt.imshow(d)
            print('Done')
            #La imagen se guarda en la ruta de salida.
            plt.savefig(os.path.basename(images[i]) + ".png")
            plt.close()
            #plt.show()
            
                
        #image_np = np.array(Image.open(path))
        
        #if store is not None:
        #    image_np = store.proxy(image_np)
            
      
        #future = gce.submit(detector, image_np)

        #print(future.result())
    
    print(f'Time: {time.perf_counter() - start:.2f}')

    if store is not None:
         store.close()
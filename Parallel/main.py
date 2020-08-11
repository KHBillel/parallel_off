import multiprocessing as mp
from PIL import Image
import os
import rawpy
import imageio
import time


def compress(root,file_name) :
    p=os.path.join(root,file_name)
    with rawpy.imread(p) as raw :
        rgb=raw.postprocess()
        
    img=Image.fromarray(rgb)
    img.save('./out/'+file_name.split('.')[0]+'.jpg',"JPEG",quality=50,optimize=True)
    
    return None
                
            
def get_filenames():
    for root, _, f in os.walk("./dataset") :
        pass
    
    return root , f


if __name__ == "__main__" :
    root,files = get_filenames()
    NCPU=mp.cpu_count()
    print(files)

    t1=time.time()
    pool=mp.Pool(processes=NCPU)
    _=pool.starmap_async(compress,[(root,file) for file in files])
    pool.close()
    pool.join()
    '''for file in files :
        compress(root,file)'''
    print("It takes " , time.time()-t1,"seconds")
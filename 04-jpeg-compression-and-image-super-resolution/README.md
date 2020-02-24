# Jpeg compression and image super-resolution

This is an experiment about deep learning. If not specified, the default dataset is `pixiv_ranking`, which is crawled from pixiv and used to train my own [StyleGAN](https://github.com/nvlabs/stylegan) anime-face model.

## Jpeg compression PSNR table

The compressed jpeg image is generated using the following code:

```python
from PIL import Image
# img = Image.open(...)
img.save(fp, format='JPEG', quality=quality)
```

|Quality|PSNR mean|PSNR std|PSNR max|PSNR min|
|:--:|:--:|:--:|:--:|:--:|
|10|26.23066|2.26392|38.90353|10.02318|
|20|28.49673|2.54121|41.75656|10.03377|
|30|29.84198|2.63707|42.09788|10.25339|
|40|30.74652|2.68095|44.26078|10.39681|
|50|31.47544|2.70604|44.82867|10.28154|
|60|32.19281|2.71910|45.18774|10.13900|
|70|33.16046|2.76882|48.90215|10.14436|
|80|34.52087|2.87345|69.30541|10.23619|
|90|36.70875|3.16596|57.46944|10.23432|

## TODO

Write a script to run the PSNR evaluation of [waifu2x-caffe](https://github.com/lltcggie/waifu2x-caffe) and train a [SR DenseNet](https://github.com/kweisamx/TensorFlow-SR-DenseNet) model and run the same evaluation, then make a comparison.

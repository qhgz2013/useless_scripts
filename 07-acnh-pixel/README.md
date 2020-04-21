# AC:NH Pixel

Animal Crossing: New Horizons pixel artwork helper, to help painting with minimal pain.

## Detail

Preview image:  
Illustrator: [光崎](https://www.pixiv.net/users/73152)

![](https://cdn.zhouxuebin.club/data/2020/04/preview.png)

Generated files:

![](https://cdn.zhouxuebin.club/data/2020/04/2020-04-15%20155600.png)

Corresponded in-game painting:

![](https://cdn.zhouxuebin.club/data/2020/04/2020-04-15%20155915.png)

## Parameters

- `--input` or `-i`: image path
- `--output` or `-o`: output path (for generated slice files, preview images, etc.)
- `--align` or `-a`: whether padding image to 32x size
- `--resize_max`: resize the maximum of width and height of the image to specified pixels
- `resize_h`: resize the height of the image to specified pixels
- `resize_w`: resize the width of the image to specified pixels

## Required site packages

- `Pillow`
- `scikit-image`
- `numpy`

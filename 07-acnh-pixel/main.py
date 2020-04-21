import argparse
from PIL import Image
from io import BytesIO
import numpy as np
import os
import skimage.color
from typing import *


def rgb_to_hsv(img: np.ndarray) -> np.ndarray:
    return np.round(skimage.color.rgb2hsv(img) * 255).astype('uint8')


def hsv_to_rgb(img: np.ndarray) -> np.ndarray:
    return np.round(skimage.color.hsv2rgb(img) * 255).astype('uint8')


def hsv_to_acnh_index(hsv: np.ndarray) -> np.ndarray:
    # HSV from ACNH:
    # H: 0 deg to 360 deg, with 30 uniform distributed values, ranging from 0 (0 deg) to 29 (348 deg)
    # S: 0% to 97%, with 15 uniform distributed values, ranging from 0 (0%) to 14 (97%)
    # V: 5% to 90%, with 15 uniform distributed values, ranging from 0 (5%) to 14 (90%)
    # so, S and V will be truncated to fit ACNH color space
    h, s, v = np.dsplit(hsv, 3)
    dh = np.round(h.astype(np.float) / 255 * 30).astype('uint8') % 30
    ds = np.round(np.clip(s.astype(np.float), 0, 255*0.97) / (255*0.97) * 14).astype('uint8')
    dv = np.round((np.clip(v.astype(np.float), 255*0.05, 255*0.9) - 255*0.05) / (255*0.85) * 14).astype('uint8')
    discrete_hsv_index = np.dstack([dh, ds, dv])
    return discrete_hsv_index


def generate_discrete_color_img(img: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    hsv = rgb_to_hsv(img)
    discrete_hsv_index = hsv_to_acnh_index(hsv)
    dh, ds, dv = np.dsplit(discrete_hsv_index, 3)
    converted_h = (dh.astype(np.float) / 30 * 255).astype('uint8')
    converted_s = (ds.astype(np.float) / 14 * (255*0.97)).astype('uint8')
    converted_v = (dv.astype(np.float) / 14 * (255*0.85) + 255*0.05).astype('uint8')
    discrete_hsv = np.dstack([converted_h, converted_s, converted_v])
    rgb = hsv_to_rgb(discrete_hsv)
    return rgb, discrete_hsv_index


def dump_data(img: np.ndarray, output_dir: str):
    with open(os.path.join(output_dir, 'source.png'), 'wb') as f:
        Image.fromarray(img).save(f, 'PNG')
    target, index = generate_discrete_color_img(img)
    preview = np.zeros_like(img, dtype='uint8')
    with open(os.path.join(output_dir, 'discrete_transformed.png'), 'wb') as f:
        Image.fromarray(target).save(f, 'PNG')
    block_y = int(np.ceil(img.shape[0] / 32))
    block_x = int(np.ceil(img.shape[1] / 32))
    print('Global Statistics:')
    print('Block X:', block_x)
    print('Block y:', block_y)
    print('Block count:', block_x*block_y)
    for y in range(block_y):
        for x in range(block_x):
            block_img = target[y*32:(y+1)*32, x*32:(x+1)*32, :]
            pil_img = Image.fromarray(block_img)
            # reduce palette to 15
            palette_img = pil_img.convert('P', palette=Image.ADAPTIVE, colors=15)
            palette = np.asarray(palette_img.getpalette(), dtype='uint8').reshape([-1, 3])
            idx = np.asarray(palette_img, dtype='uint8')
            block_img = palette[idx, :]
            preview[y*32:(y+1)*32, x*32:(x+1)*32, :] = block_img
            idx_in_used = set()
            for _, pixel_idx in np.ndenumerate(idx):
                idx_in_used.add(int(pixel_idx))
            palette_hsv = rgb_to_hsv(np.expand_dims(palette, 0))
            palette_acnh_idx = hsv_to_acnh_index(palette_hsv).squeeze(0)
            # writing block data
            with open(os.path.join(output_dir, 'block-%d-%d.txt' % (y, x)), 'w') as f:
                f.write('Block x: %d\n' % x)
                f.write('Block y: %d\n' % y)
                f.write('Palette (HSV/HSB color space):\nIndex  Hue  Saturation  Brightness\n')
                for i in idx_in_used:
                    f.write('%5d  %3d  %10d  %10d\n' % (i, int(palette_acnh_idx[i, 0]),
                                                        int(palette_acnh_idx[i, 1]), int(palette_acnh_idx[i, 2])))
                f.write('Palette order of 32x32 image:\n')
                for pixel_y in range(block_img.shape[0]):
                    for pixel_x in range(block_img.shape[1]):
                        f.write('%2d ' % idx[pixel_y, pixel_x])
                        if pixel_x % 5 == 4:
                            f.write('| ')
                    f.write('\n')
                    if pixel_y % 5 == 4:
                        for pixel_x in range(block_img.shape[1]):
                            f.write('---')
                            if pixel_x % 5 == 4:
                                f.write('+-')
                        f.write('\n')
            with open(os.path.join(output_dir, 'block-%d-%d.png' % (y, x)), 'wb') as f:
                Image.fromarray(block_img).save(f, 'PNG')
    with open(os.path.join(output_dir, 'preview.png'), 'wb') as f:
        Image.fromarray(preview).save(f, 'PNG')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', type=str, required=True, help='Input image path')
    parser.add_argument('--output', '-o', type=str, required=True, help='Output directory')
    parser.add_argument('--align', '-a', help='Align input image to 32x size', default=False, const=True,
                        action='store_const')
    parser.add_argument('--resize_max', type=int, help='The maximum w&h pixels that the image resized to')
    parser.add_argument('--resize_h', type=int, help='The height of the resized image')
    parser.add_argument('--resize_w', type=int, help='The width of the resized image')
    args = parser.parse_args()
    # read input image
    with open(args.input, 'rb') as f:
        img_blob = f.read()
    with BytesIO(img_blob) as f:
        img = Image.open(f)
        img = np.asarray(img, dtype='uint8')
        if len(img.shape) < 3 or img.shape[2] != 3:
            img = Image.fromarray(img).convert('RGB')
            img = np.asarray(img, dtype='uint8')
        assert img.shape[2] == 3, 'Unsupported image format, RGB image is required'
    os.makedirs(args.output, exist_ok=True)
    # align image
    if args.align:
        pad_h = 32 - (img.shape[0] - (img.shape[0] // 32) * 32)
        pad_w = 32 - (img.shape[1] - (img.shape[1] // 32) * 32)
        if pad_h > 0 or pad_w > 0:
            new_img = np.zeros([img.shape[0] + pad_h, img.shape[1] + pad_w, img.shape[2]], dtype='uint8')
            h = int(pad_h // 2)
            w = int(pad_w // 2)
            new_img[h:h+img.shape[0], w:w+img.shape[1], :] = img
            img = new_img
    # resize image
    if args.resize_max:
        max_size = np.max(img.shape[:2])
        ratio = args.resize_max / max_size
    elif args.resize_h:
        ratio = args.resize_h / img.shape[0]
    elif args.resize_w:
        ratio = args.resize_w / img.shape[1]
    else:
        ratio = 1.0
    if ratio != 1.0:
        new_h, new_w = np.round(np.array(img.shape[:2], dtype=np.float) * ratio)
        img = np.asarray(Image.fromarray(img).resize((int(new_w), int(new_h)), Image.ANTIALIAS), dtype='uint8')

    dump_data(img, args.output)


if __name__ == '__main__':
    main()

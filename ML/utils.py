# from fastai.data.transforms import get_image_files
# from fastai.vision.all import *


# def get_dataloader(base_pah, image_path, batch_size, img_size) -> ImageDataLoaders:
#     """Returns a dataloader for the given image paths and batch size.
#     Adapted from the fastai tutorial"""
#     filenames = get_image_files(image_path)
#     dls = ImageDataLoaders.from_name_re(
#         base_pah, image_path, pat=r'(.+)_\d+.jpg$', item_tfms=Resize(460), bs=batch_size,
#         batch_tfms=[*aug_transforms(size=224, min_scale=0.75), Normalize.from_stats(*imagenet_stats)])

#     return dls

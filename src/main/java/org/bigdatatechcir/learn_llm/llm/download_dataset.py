import os
import json
from tqdm import tqdm

# 下载预训练数据集
# os.system("modelscope download --dataset ddzhu123/seq-monkey mobvoi_seq_monkey_general_open_corpus.jsonl.tar.bz2 --local_dir ./autodl-tmp/dataset/pretrain_data")
# # 解压预训练数据集
# os.system("tar -xvf ./autodl-tmp/dataset/pretrain_data/mobvoi_seq_monkey_general_open_corpus.jsonl.tar.bz2")

# 设置环境变量
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'
# 下载SFT数据集
os.system(f'huggingface-cli download --repo-type dataset --resume-download BelleGroup/train_3.5M_CN --local-dir ./autodl-tmp/dataset/sft_data/BelleGroup')

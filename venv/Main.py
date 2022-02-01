import os
import math
import functools
from multiprocessing import Pool


folder_name = "wiki"
file_names = os.listdir(folder_name)
first_file = file_names[0]
lines_total = 0
lines_all = []

print('Number of files in folder: ', len(file_names))

for file_name in file_names:
    with open(os.path.join(folder_name, file_name)) as f:
        for line in f.readlines():
            lines_all.append(line)

print(type(file_names))
print('Total amount of lines in files: ', len(lines_all))
print('head of all lines: ', lines_all[:10])


def make_chunks(data, num_chunks):
    chunk_size = math.ceil(len(data) / num_chunks)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def mapper(num_chunks):
    return len(num_chunks)


def reducer(amount1, amount2):
    return amount1 + amount2


def map_reduce(data, num_processes, mapper, reducer):
    chunks = make_chunks(data, num_processes)
    pool = Pool(num_processes)
    chunk_results = pool.map(mapper, chunks)
    return functools.reduce(reducer, chunk_results)


test = map_reduce(lines_all, 4, mapper, reducer)

print(test)


def make_chunks(data, num_chunks):
    chunk_size = math.ceil(len(data) / num_chunks)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def mapper(chunks, target):
    grep = {}
    remainder_inc = 0
    remainder_dec = 0
    for ind in range(0, len(chunks)):
        chunk = chunks[ind]
        file_name = file_names[ind]
        if chunk.count(target) > 0:
            if len(chunk) > len(file_name):
                ind_map = len(chunk) // len(file_name)
                if file_names[ind_map] not in grep:
                    grep[file_names[ind_map]] = [0]
                else:
                    grep[file_names[ind_map]].append(chunk.count(target))


            elif len(chunk) <= len(file_name) and (len(file_name) // len(chunk)) == 1:

                if file_name not in grep:
                    grep[file_name] = [0]
                else:
                    grep[file_name].append(chunk.count(target))
            else:
                ind_map = (len(file_name) // len(chunk))
                if file_name not in grep:
                    grep[file_names[ind - ind_map]] = [0]
                else:
                    grep[file_names[ind - ind_map]].append(chunk.count(target))
    return grep


chunks = make_chunks(lines_all, 100)
print(chunks[0])
# def reducer(greptable1, greptable2):
#     for key in greptable2:
#         if key in greptable1:
#             greptable1[key] = greptable1[key] + greptable2[key]
#     return greptable1

# def map_reduce(data, num_processes, mapper, reducer):
#     chunks = make_chunks(data, num_processes)
#     pool = Pool(num_processes)
#     chunk_results = pool.map(mapper, chunks)
#     return functools.reduce(reducer, chunk_results)

# grep = map_reduce(lines_all, 6, )
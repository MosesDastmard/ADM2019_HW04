import numpy as np
#%%
def counting_sort(array):
    aux_array = [0]*1000
    order = [0]*len(array)
    for i in array:
        aux_array[i] += 1
    for i in range(1,len(aux_array)):
        aux_array[i] += aux_array[i-1]
    sorted_array = [None]*len(array)
    for j,i in enumerate(array):
        order[aux_array[i]-1] = j
        sorted_array[aux_array[i]-1] = i
        aux_array[i] += -1
    return sorted_array, order
#%%
def subproblem(sorted_array, index_array):
    problem_list = list()
    problem = [index_array[0]]
    for i in range(1,len(sorted_array)):
        if sorted_array[i] == sorted_array[i-1]:
            problem.append(index_array[i])
        else:
            problem_list.append(problem)
            problem = [index_array[i]]
    problem_list.append(problem)
    return problem_list

#%%
def alpha_rec(arrays, i):
    if arrays.shape[0] == 1:
        return arrays
    else:
        sorted_array, order = counting_sort(arrays[:,i])
        arrays = arrays[order,:]
        if (i+1) < arrays.shape[1] :
            list_subproblems = subproblem(sorted_array,list(range(arrays.shape[0])))
            list_array = list()
            for problem in list_subproblems:
                list_array.append(alpha_rec(arrays[problem,:], i+1))
            arrays = np.concatenate(list_array, axis = 0)
        return arrays
#%%
def word2array(words_list):        
    max_len = max(map(len, words_list))
    words_num = len(words_list)
    arrays = np.zeros((words_num, max_len+1), dtype=int)
    arrays[:,-1] = range(words_num)
    for i,word in enumerate(words_list):
        arrays[i,range(len(word))] = list(map(ord, word.lower()))
    return arrays
#%%    
def alpha_counting_sort(words_list):
    arrays = word2array(words_list)
    sorted_array = alpha_rec(arrays, 0)
    order = list(map(int, sorted_array[:,-1]))
    return([words_list[i] for i in order])

### The MILP problems can be solved using pulp packages that powered by B&B algorithm
### provides the ability to get the global optimum for not only LP but also MILP
import pandas as pd
from numpy.linalg import norm
import numpy as np
import pulp as plp
#%%
def dist(df):
    dist_mat = np.zeros((df.shape[0], df.shape[0]))
    for i in range(df.shape[0]):
        for j in range(df.shape[0]):
            dist_mat[i,j] = norm(df.iloc[i,:].values - df.iloc[j,:].values)**2
    return dist_mat
def k_median(df, K):
    dist_mat = dist(df)
    n = df.shape[0]
    k = K
    set_I = range(n)
    set_J = range(n)
    opt_model = plp.LpProblem(name="MILP_k-median_Model")
    # if x is Binary
    x_vars  =  {"x_{0}_{1}".format(i,j): plp.LpVariable(cat = plp.LpBinary, name="x_{0}_{1}".format(i,j)) for i in set_I for j in set_J}
    y_vars  =  {"y_{0}".format(j): plp.LpVariable(cat = plp.LpBinary, name="y_{0}".format(j)) for j in set_J}
    vars_   = {**x_vars, **y_vars}
    # == constraints
    con1 = {"con1_{0}".format(i) :
            opt_model.addConstraint(plp.LpConstraint(
                e = plp.lpSum(vars_["x_{0}_{1}".format(i,j)] 
                              for j in set_J), 
                sense = plp.LpConstraintEQ,
                rhs = 1,
                name="con1_{0}".format(i))) 
            for i in set_I}
        
    con2 = {"con2" : opt_model.addConstraint(plp.LpConstraint(e = plp.lpSum(vars_["y_{0}".format(j)] for j in set_J), sense = plp.LpConstraintEQ , rhs = k, name="con2"))}
                                             
                                             
    con3 = {"con3_{0}_{1}".format(i,j) : opt_model.addConstraint(plp.LpConstraint(e = plp.lpSum([vars_["x_{0}_{1}".format(i,j)], -vars_["y_{0}".format(j)]]), sense = plp.LpConstraintLE, rhs = 0,name="con3_{0}_{1}".format(i,j))) for i in set_I for j in set_J}
    con = {**con1, **con2, **con3}
    objective = plp.lpSum(vars_["x_{0}_{1}".format(i,j)] * dist_mat[i,j] 
                          for i in set_I 
                          for j in set_J)
    # for minimization
    opt_model.sense = plp.LpMinimize
    opt_model.setObjective(objective)
    
    # solving with local cplex
    opt_model.solve()
    
    opt_model.numConstraints()
    
    opt_df = pd.DataFrame.from_dict(x_vars, orient="index", 
                                    columns = ["variable_object"])
    
    opt_df["solution_value"] = opt_df["variable_object"].apply(lambda item: item.varValue)
    
    opt_df['i'] = opt_df['variable_object'].apply(lambda x: int(x.name.split("_")[1]))
    opt_df['j'] = opt_df['variable_object'].apply(lambda x: int(x.name.split("_")[2]))
    
    
    return (pd.pivot_table(data = opt_df, values='solution_value', index ='i', columns='j').values*dist_mat).sum()
    
#%%
def plot_res():
    p1 =       40.41
    p2 =      -816.4
    p3 =        6109
    p4 =  -2.054e+04
    p5 =    2.95e+04
    p6 =  -1.069e+04
    
    def f(t):
        return p1*t**5 + p2*t**4 + p3*t**3 + p4*t**2 + p5*t**1 + p6
    
    import matplotlib.pyplot as plt
    x = np.arange(2,7,.1)
    y = [f(t) for t in x]
    #print(x)
    #print(y)
    plt.figure(figsize=(10,6))
    plt.ylim(1200,3250)
    plt.plot(x,y)
    plt.scatter(x[10],y[10], marker="d", lw = 5, color = 'blue')
    plt.scatter(x[12],y[12], marker="*", lw = 5, color = 'r')
    plt.scatter(x[43],y[43], marker="o", lw = 5, color = 'green')
    plt.grid(color='r', linestyle='-', linewidth=.1, axis='x')
    plt.text(x=2.1, y=1556, s='k-median', color = 'blue')
    plt.text(x=3.1, y=1356, s='Global minimum', color = 'r')
    plt.text(x=6.2, y=2276, s='k-means', color = 'green')
    plt.ylabel('Cost')
    plt.xticks([1,2,3,4,5,6,7],["","","","","","","",""])
    #print(np.array(y).argmin())
    

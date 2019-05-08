import os, sys
from .preprocessing import preprocess_file



def cos_sim(A,B):
	return( sum(A*B)/sqrt(sum(A^2)*sum(B^2)) )   

if __name__ == '__main__':
    input_file = sys.args[1]
    df = preprocess_file(input_file)
    item_id = 0


    # 1. convert to rdd (user_id, (item_id, rating))
    rdd = df.rdd

    user_rdd = rdd.map(lambda tup: (tup[0], (tup[1], tup[2])))
    # 2. groupBy Key
    user_groups = item_rdd.groupByKey()
    # 3. for item_id and user_id, get prediction

    # get only users

    
    # 4. move to other file


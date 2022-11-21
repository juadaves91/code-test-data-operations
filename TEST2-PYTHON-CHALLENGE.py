# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <div style="background-color:#89afc7;height:230px;color:white">
# MAGIC <div style="padding-left: 10px;float:left;width:50%;height:200px;color:white">
# MAGIC <h2>Data Engineer Test</h2>
# MAGIC 
# MAGIC <b>Juan David Escobar Escobar.</b>
# MAGIC * Oct 2022, Medellin, Colombia.
# MAGIC * https://www.linkedin.com/in/jdescobar/
# MAGIC * https://github.com/juadaves91/unir-tfm-alzheimer-diagnostic-deep-learning
# MAGIC </div>
# MAGIC 
# MAGIC <div style="float:right;width:50%;height:200px">
# MAGIC   <div style="float:right;padding-top: 15px;padding-right: 10px">
# MAGIC <img style="border-radius: 50%" src="https://media-exp1.licdn.com/dms/image/C5603AQHhxR-QKjuTYw/profile-displayphoto-shrink_200_200/0/1661897414952?e=1671062400&v=beta&t=QFDVjmUPM-8UyURqIvppyoLnrgZS1LJDmfskgCSDDzU">
# MAGIC     </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <p>
# MAGIC <b>Spark & Python Coding Challenge</b></br>
# MAGIC Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target. # You may assume that each input would have exactly one solution, and you may </br>
# MAGIC not use the same element twice. # You can return the answer in any order. </br>
# MAGIC <b>Example 1: # Input: nums = [2,7,11,15], </b></br>
# MAGIC                          target = 9 # Output: [0,1] </br>
# MAGIC                      # Output: Because nums[0] + nums[1] == 9, </br>
# MAGIC                          we return [0, 1].</br>
# MAGIC <b>Example 2: # Input: nums = [3,2,4],</b></br>
# MAGIC                          target = 6 </br>
# MAGIC                        # Output: [1,2]</br>
# MAGIC </p>

# COMMAND ----------

def fn_calculate_target(arr_input: list, target: int):
    nx_index = 0
    list_result = []
    for index, num in enumerate(arr_input):
        arr_input_tmp = arr_input[index + 1: ]
        nx_num = next((nx_num for nx_num in arr_input_tmp if ((num + nx_num) == target)), -1)
    
        if nx_num > -1:
            nx_index = arr_input.index(nx_num)
            list_result.append(index)
            list_result.append(nx_index)
            break 
    
    return list_result

# COMMAND ----------

assert fn_calculate_target([2,7,11,15], 9) == [0, 1]
assert fn_calculate_target([3,2,4], 6) == [1, 2]

# COMMAND ----------

import unittest


class TestSum(unittest.TestCase):

    def test_calculate_target(self):
      list_test = [{
                      'input' : [2,7,11,15],
                      'target' : 9,
                      'result' : [0, 1]
                   },
                   {
                      'input' : [3,2,4],
                      'target' : 6,
                      'result' : [1, 2]
                   }]

      for current_test in list_test:
        current_result = fn_calculate_target(current_test['input'], current_test['target'])  
        print(type(current_result), current_result, type(current_test['result']), current_test['result'])
        self.assertEqual(current_result, current_test['result'], "Should be " + str(current_test['result']))
        

if __name__ == '__main__':
  unittest.main(argv=['first-arg-is-ignored'], exit=False)





# arr = [1, 1, 1, 64, 23, 64, 22, 22, 22]
# size = len(arr)
 
# 	# looping till length - 2
# for i in range(size - 2):

#         # checking the conditions
#         if arr[i] == arr[i + 1] and arr[i + 1] == arr[i + 2]:

#             # printing the element as the
#             # conditions are satisfied
#             print(arr[i])


# def _encode(sample_string):
#     import base64
#     sample_string_bytes = sample_string.encode("ascii")
  
#     base64_bytes = base64.b64encode(sample_string_bytes)
#     base64_pwd = base64_bytes.decode("ascii")
#     print(base64_pwd)
# _encode('G1-FQ1QM13-L\SQLEXPRESS')

# from multipledispatch import dispatch 
# # passing one parameter

# @dispatch(int, int)
# def product(first, second):
#     result = first*second
#     print(result)
 
# # passing two parameters
# @dispatch(int, int, int)
# def product(first, second, third):
#     result = first * second * third
#     print(result)
 
# # you can also pass data type of any value as per requirement 
# @dispatch(float, float, float)
# def product(first, second, third):
#     result = first * second * third
#     print(result)
 
# # calling product method with 2 arguments
# product(2, 3)  # this will give output of 6
 
# # calling product method with 3 arguments but all int
# product(2, 3, 2)  # this will give output of 12
 
# # calling product method with 3 arguments but all float
# product(2.2, 3.4, 2.3)  # this will give output of 17.985999999999997
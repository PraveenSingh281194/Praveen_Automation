from enum import Enum

class db(Enum):
     a="Mysql"
     b="ssms"
     c="postgres"
     d="oracle"

print(list(db))

# [
#     Size.S: 'small'>,
#     Size.M: 'medium'>,
#     Size.L: 'large'>,
#     <Size.XL: 'extra large'>
# ]
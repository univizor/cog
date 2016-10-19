var numbers = List(
  22, 12, 24, 43, 12,
  433, 29, 44, 166, 2,
  12, 3, 4, 7, 3, 1
)

numbers = (0 to 10).toList

def odd(n: Int) = n % 2 == 0
def even(n: Int) = !odd(n)

numbers.filter(even)
  .flatMap(i => numbers.filter(odd)
    .map(j => i * j)
  )
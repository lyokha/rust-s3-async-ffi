NAME := s3_async_test
SRC := $(NAME).cpp
BOOST_PO_LIB := -lboost_program_options
RUST_LIB := -L../../target/debug -lrusts3asyncffi

all: $(NAME)

$(NAME): $(SRC)
	g++ -g -Wall -o $(NAME) $(SRC) $(BOOST_PO_LIB) $(RUST_LIB)

clean:
	rm $(NAME)


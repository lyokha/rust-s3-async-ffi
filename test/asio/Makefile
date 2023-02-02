NAME := s3_async_test
SRC := $(NAME).cpp
RUST_LIB_DIR := ../../target/debug
RUST_LIB_NAME := rusts3asyncffi
RUST_LIB := $(RUST_LIB_DIR)/lib$(RUST_LIB_NAME).so
LINK_RUST_LIB := -L$(RUST_LIB_DIR) -l$(RUST_LIB_NAME)
LINK_BOOST_PO_LIB := -lboost_program_options

all: $(NAME)

$(NAME): $(SRC) $(RUST_LIB)
	g++ -g -Wall -o $(NAME) $(SRC) $(LINK_BOOST_PO_LIB) $(LINK_RUST_LIB)

clean:
	rm $(NAME)


# define the name of the virtual environment directory
VENV := venv

# default target, when make executed without arguments
all: venv

$(VENV)/bin/activate: requires_install.txt
	python3.9 -m venv $(VENV)
	./$(VENV)/bin/pip3 install -r requires_install.txt

# venv is a shortcut target
venv: $(VENV)/bin/activate



.PHONY: all venv
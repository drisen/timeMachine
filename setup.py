import setuptools

with open("README.md", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name="timeMachine",
	version="2.0.1",
	author="Dennis Risen",
	author_email="dar5@case.edu",
	description="Table with indices of record contents by time",
	long_description=long_description,
	long_description_content_type="text/markdown",
	#url="https://github.com/pypa/sampleproject",
	packages=setuptools.find_packages(),
	classifiers=[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	],
	python_requires='>=3.6',
)

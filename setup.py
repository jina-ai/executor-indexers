import setuptools

setuptools.setup(
    name="jina-hub-indexers",
    version="0.0.1",
    author='Jina Dev Team',
    author_email='dev-team@jina.ai',
    description="A set of indexers for Jina",
    url="https://github.com/jina-ai/indexers",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages=['dbms', 'dump', 'query'],
    python_requires=">=3.7",
)
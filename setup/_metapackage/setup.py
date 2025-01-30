import setuptools

with open('VERSION.txt', 'r') as f:
    version = f.read().strip()

setuptools.setup(
    name="odoo9-addons-akretion-synchronizer",
    description="Meta package for akretion-synchronizer Odoo addons",
    version=version,
    install_requires=[
        'odoo9-addon-synchronizer',
    ],
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Odoo',
        'Framework :: Odoo :: 9.0',
    ]
)

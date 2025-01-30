import setuptools

with open('VERSION.txt', 'r') as f:
    version = f.read().strip()

setuptools.setup(
    name="odoo-addons-akretion-synchronizer",
    description="Meta package for akretion-synchronizer Odoo addons",
    version=version,
    install_requires=[
        'odoo-addon-synchronizer>=16.0dev,<16.1dev',
    ],
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Odoo',
        'Framework :: Odoo :: 16.0',
    ]
)

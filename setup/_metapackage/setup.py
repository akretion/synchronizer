import setuptools

with open('VERSION.txt', 'r') as f:
    version = f.read().strip()

setuptools.setup(
    name="odoo14-addons-akretion-synchronizer",
    description="Meta package for akretion-synchronizer Odoo addons",
    version=version,
    install_requires=[
        'odoo14-addon-synchronizer',
    ],
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Odoo',
        'Framework :: Odoo :: 14.0',
    ]
)

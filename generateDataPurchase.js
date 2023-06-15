const faker = require('faker');

// Generate a list of random products
var productsList = [];
var companyList = [];
var batchSize;

function init() {
    productListSize = process.env.SIZE_LIST_PRODUCTS;
    if (typeof productListSize === 'undefined') { productListSize = 1000 }
    companyListSize = process.env.SIZE_LIST_COMPANIES;
    if (typeof companyListSize === 'undefined') { companyListSize = 1000 }
    batchSize = process.env.SIZE_BATCH;
    if (typeof batchSize === 'undefined') { batchSize = 200 }

    for (let i = 0; i < productListSize; i++) {
        const product = {
            productdepartment: faker.commerce.department(),
            productname: faker.commerce.productName(),
            productprice: Number(faker.commerce.price()),
            productquantity: 0,
            productsubtotal: 0
        };
        productsList.push(product);
    }
    for (let i = 0; i < companyListSize; i++) {
        const company = {
            companyname: faker.company.companyName(),
            billingAddress: faker.address.streetAddress() + ', ' + faker.address.city() + ', ' + faker.address.country()
        };
        companyList.push(company);
    }
        
}

// Generate a random message with name, address, and a random number of products
function generatePurchase() {
    const randomCompanyIndex = Math.floor(Math.random() * companyList.length); // Generate a random index for the productsList array
    const company = companyList[randomCompanyIndex]; // Use the random index to select a random product from the list
    const name = faker.name.findName();
    const deliveryaddress = faker.address.streetAddress() + ', ' + faker.address.city() + ', ' + faker.address.country();
    const products = [];
    const numProducts = Math.floor(Math.random() * 10) + 1; // Generate a random number of products between 1 and 10
    var producttotal = 0;
    for (let i = 0; i < numProducts; i++) {
        const randomProductIndex = Math.floor(Math.random() * productsList.length); // Generate a random index for the productsList array
        const product = productsList[randomProductIndex]; // Use the random index to select a random product from the list
        product.productquantity = Math.floor(Math.random() * 5) + 1; // Generate a random number of products between 1 and 10
        product.productsubtotal = product.productquantity * product.productprice;
        producttotal = producttotal + product.productsubtotal;
        products.push(product);
    }
    return {
        company: company,
        name: name,
        deliveryaddress: deliveryaddress,
        products: products,
        payableamount: producttotal
    };
}

// Generate a batch of random messages with name, address, and a random number of products
function generatePurchaseBatch() {
    const purchasebatch = [];
    for (let i = 0; i < batchSize; i++) {
        var batchItem = generatePurchase();
        purchasebatch.push(batchItem);
    }
    return purchasebatch;
}

exports.generatePurchase = generatePurchase;
exports.generatePurchaseBatch = generatePurchaseBatch;

init();
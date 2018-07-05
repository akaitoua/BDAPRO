if (window.console) {
  console.log("Welcome to your Play application's JavaScript!");
}

var selected_id = null

function select_id(id) {
    selected_id = id
}

$('#pagination-demo').twbsPagination({
    totalPages: 35,
    visiblePages: 7,
    onPageClick: function (event, page) {
        $('#page-content').text('Page ' + page);
    }
});

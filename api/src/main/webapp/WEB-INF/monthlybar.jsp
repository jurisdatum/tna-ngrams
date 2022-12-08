<!doctype html>
<html>
<head>

<style>

body { font-family: Helvetica, sans-serif }

h1 { margin-bottom: 6pt; text-align: center }
h2 { margin-top: 6pt; text-align: center }

.axis path,
.axis line {
	fill: none;
	stroke: #000;
	shape-rendering: crispEdges
}

</style>

</head>

<body>

<h1>${ngrams}</h1>

<script src="/lib/d3.js"></script>

<script>

function BarGraph() {

	this.margin = {top: 20, right: 20, bottom: 30, left: 40};
	this.width = 860 - this.margin.left - this.margin.right;
	this.height = 400 - this.margin.top - this.margin.bottom;

	this.x0 = d3.scale.ordinal()
		.rangeRoundBands([0, this.width], .1);

	this.x1 = d3.scale.ordinal();

	this.y = d3.scale.linear()
		.range([this.height, 0]);

	this.color = d3.scale.category10();

	var months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec'];

	this.xAxis = d3.svg.axis()
		.scale(this.x0)
		.orient("bottom")
		.tickFormat(function(v) { return months[v]; });

	this.yAxis = d3.svg.axis()
		.scale(this.y)
		.orient("left")
		.tickFormat(d3.format(".2s"));
}
BarGraph.prototype.data = function(data, xKey) {

	this.data = data;
	this.xKey = xKey;

	this.yKeys = d3.keys(data[0].counts);

	var that = this;
	data.forEach(function(d) {
		d.values = that.yKeys.map(function(name) { return { name: name, value: d.counts[name] }; });
	});
	
	this.x0.domain(data.map(function(d) { return d[xKey]; }));
	this.x1.domain(this.yKeys).rangeRoundBands([0, this.x0.rangeBand()]);
	this.y.domain([0, d3.max(data, function(d) { return d3.max(d.values, function(d) { return d.value; }); })]);
	return this;
};
BarGraph.prototype.add = function(element) {
	
	this.svg = element
		.append("div")
			.style('text-align', 'center')
		.append("svg")
			.attr("width", this.width + this.margin.left + this.margin.right)
			.attr("height", this.height + this.margin.top + this.margin.bottom)
		.append("g")
			.attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")");
	
	this.svg.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(0," + this.height + ")")
		.call(this.xAxis);

	this.svg.append("g")
		.attr("class", "y axis")
		.call(this.yAxis);
	
	var that = this;

	var state = this.svg.selectAll(".state")
		.data(that.data)
		.enter().append("g")
			.attr("class", "g")
			.attr("transform", function(d) { return "translate(" + that.x0(d[that.xKey]) + ",0)"; });

	state.selectAll("rect")
		.data(function(d) { return d.values; })
		.enter().append("rect")
			.attr("width", that.x1.rangeBand())
			.attr("x", function(d) { return that.x1(d.name); })
			.attr("y", function(d) { return that.y(d.value); })
			.attr("height", function(d) { return that.height - that.y(d.value); })
			.style("fill", function(d) { return that.color(d.name); });
	return this;
};
BarGraph.prototype.legend = function() {
	this.legend = this.svg.selectAll(".legend")
		.data(this.yKeys.slice())
		.enter().append("g")
			.attr("class", "legend")
			.attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

	this.legend.append("rect")
		.attr("x", this.width - 18)
		.attr("width", 18)
		.attr("height", 18)
		.style("fill", this.color);

	this.legend.append("text")
		.attr("x", this.width - 24)
		.attr("y", 9)
		.attr("dy", ".35em")
		.style("text-anchor", "end")
		.text(function(d) { return d; });
	return this;
};

</script>


<script>

var data = ${data};

data.forEach(function(d, i) {
	d3.select('body').append('h2').text(d.year);
	var graph = new BarGraph().data(d.months, 'month').add(d3.select('body'));
	if (i === 0) graph.legend();	
});

</script>

</body>
</html>

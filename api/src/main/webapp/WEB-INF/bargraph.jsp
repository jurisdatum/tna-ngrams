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
<h2>${dates}</h2>

<script src="/lib/d3.js"></script>
<script>

var margin = {top: 20, right: 20, bottom: 30, left: 40},
	width = 860 - margin.left - margin.right,
	height = 400 - margin.top - margin.bottom;

var x0 = d3.scale.ordinal()
	.rangeRoundBands([0, width], .1);

var x1 = d3.scale.ordinal();

var y = d3.scale.linear()
	.range([height, 0]);

var color = d3.scale.category10();

var xAxis = d3.svg.axis()
	.scale(x0)
	.orient("bottom");

var yAxis = d3.svg.axis()
	.scale(y)
	.orient("left")
	.tickFormat(d3.format(".2s"));

var svg = d3.select("body")
	.append("div")
		.style('text-align', 'center')
	.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
	.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");


var data = ${data};

var ageNames = d3.keys(data[0].counts);

data.forEach(function(d) {
	d.ages = ageNames.map(function(name) { return { name: name, value: d.counts[name] }; });
});

x0.domain(data.map(function(d) { return d.year; }));
x1.domain(ageNames).rangeRoundBands([0, x0.rangeBand()]);
y.domain([0, d3.max(data, function(d) { return d3.max(d.ages, function(d) { return d.value; }); })]);

svg.append("g")
	.attr("class", "x axis")
	.attr("transform", "translate(0," + height + ")")
	.call(xAxis);

svg.append("g")
	.attr("class", "y axis")
	.call(yAxis);

var state = svg.selectAll(".state")
	.data(data)
	.enter().append("g")
		.attr("class", "g")
		.attr("transform", function(d) { return "translate(" + x0(d.year) + ",0)"; });

state.selectAll("rect")
	.data(function(d) { return d.ages; })
	.enter().append("rect")
		.attr("width", x1.rangeBand())
		.attr("x", function(d) { return x1(d.name); })
		.attr("y", function(d) { return y(d.value); })
		.attr("height", function(d) { return height - y(d.value); })
		.style("fill", function(d) { return color(d.name); });

var legend = svg.selectAll(".legend")
	.data(ageNames.slice())
	.enter().append("g")
		.attr("class", "legend")
		.attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

legend.append("rect")
	.attr("x", width - 18)
	.attr("width", 18)
	.attr("height", 18)
	.style("fill", color);

legend.append("text")
	.attr("x", width - 24)
	.attr("y", 9)
	.attr("dy", ".35em")
	.style("text-anchor", "end")
	.text(function(d) { return d; });

</script>

</body>
</html>

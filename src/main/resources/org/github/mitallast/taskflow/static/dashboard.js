(function(){
    'strict';

    angular.module('dag', ['ngRoute'])
    .config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){
        $locationProvider.html5Mode(false);
        $locationProvider.hashPrefix("");
        $routeProvider
            .when('/', {
                redirectTo: '/dag/latest'
            })
            .when('/dag/latest', {
                templateUrl: '/dag-latest.html',
                controller: 'DagLatestCtrl'
            })
            .when('/dag/id/:id', {
                templateUrl: '/dag-state.html',
                controller: 'DagIdCtrl'
            })
            .when('/dag/token/:token', {
                templateUrl: '/dag-state.html',
                controller: 'DagTokenCtrl'
            })
            .when('/dag/create', {
                templateUrl: '/dag-create.html',
                controller: 'DagCreateCtrl'
            })
            .when('/dag/update/id/:id', {
                templateUrl: '/dag-update.html',
                controller: 'DagUpdateCtrl'
            })
            .when('/dag/run', {
                templateUrl: '/dag-run.html',
                controller: 'DagRunCtrl'
            })
            .when('/dag/run/pending', {
                templateUrl: '/dag-run-pending.html',
                controller: 'DagRunPendingCtrl'
            })
            .when('/dag/run/id/:id', {
                templateUrl: '/dag-run-state.html',
                controller: 'DagRunIdCtrl'
            })
            .when('/operations', {
                templateUrl: '/operations.html',
                controller: 'OperationsController'
            })
            .otherwise({
                redirectTo: '/'
            });
    }])
    .controller('SidebarCtrl', function($scope, $location){
        $scope.menus = [
            [
                {href:'dag/latest', title:'Dag latest'},
                {href:'dag/id/1', title:'Dag 1'},
                {href:'dag/create', title:'Create Dag'},
            ],
            [
                {href:'dag/run', title:'Dag run'},
                {href:'dag/run/pending', title:'Dag run pending'},
                {href:'dag/run/id/1', title:'Dag run 1'},
            ],
            [
                {href:'operations', title:'Operations'},
            ]
        ];
        $scope.activeClass = function(page){
            var current = $location.path().substring(1);
            return page === current ? "active" : "";
        };
    })
    .controller('DagLatestCtrl', function($scope, $http){
        $http.get('/api/dag/latest')
        .then(function(response){
            $scope.latest = response.data;
        });
    })
    .controller('DagIdCtrl', function($scope, $http, $routeParams){
        $http.get('/api/dag/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('DagTokenCtrl', function($scope, $http, $routeParams){
        $http.get('/api/dag/token/' + $routeParams.token)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('DagCreateCtrl', function($scope, $http, $location){
        $scope.dag = {
           token: '',
           tasks: []
        };
        $scope.errors = false;
        $scope.operations = [];
        $scope.operationsMap = {};
        $http.get('/api/operation')
            .then(function(response){
                $scope.operations = response.data.map(function(item) {
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item) {
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.addTask = function() {
            $scope.dag.tasks.push({
                token: '',
                operation: '',
                depends: [],
                command: {
                    config: {},
                    environment: {}
                }
            });
        };
        $scope.validateDag = function() {
            $http.put('/api/dag/validate', $scope.dag)
            .then(
                function(response){
                    $scope.errors = false;
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $scope.createDag = function() {
            $http.put('/api/dag', $scope.dag)
            .then(
                function(response){
                    $location.path('/dag/update/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
    })
    .controller('DagUpdateCtrl', function($scope, $http, $location, $routeParams){
        $scope.addTask = function() {
            $scope.dag.tasks.push({
                token: '',
                operation: '',
                depends: [],
                command: {
                    config: {},
                    environment: {}
                }
            });
        };
        $scope.operations = [];
        $scope.operationsMap = {};
        $http.get('/api/operation')
            .then(function(response){
                $scope.operations = response.data.map(function(item) {
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item) {
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.validateDag = function() {
            $http.put('/api/dag/validate', $scope.dag)
            .then(
                function(response){
                    $scope.errors = false;
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $scope.updateDag = function() {
            $http.post('/api/dag', $scope.dag)
            .then(
                function(response){
                    $location.path('/dag/update/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $http.get('/api/dag/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('CreateDagRunCtrl', function($scope, $http, $location){
        $scope.createDagRunById = function(id){
            $http.put('/api/dag/id/' + id + "/run")
            .then(
                function(response){
                    $location.path('/dag/run/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            )
        };
        $scope.createDagRunByToken = function(token){
            $http.put('/api/dag/token/' + token + "/run")
            .then(
                function(response){
                    $location.path('/dag/run/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            )
        };
    })
    .controller('DagRunCtrl', function($scope, $http){
        $http.get('/api/dag/run')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('DagRunPendingCtrl', function($scope, $http){
        $http.get('/api/dag/run/pending')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('DagRunIdCtrl', function($scope, $http, $routeParams){
        $http.get('/api/dag/run/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag_run = response.data;
        });
    })
    .controller('OperationsController', function($scope, $http){
        $scope.operations = [];
        $http.get('/api/operation')
        .then(function(response){
            $scope.operations = response.data;
        });
    })
    .directive('diagramGantt', function() {
        return {
            restrict: 'A',
            scope: true,
            transclude: true,
            link: function(scope, element, attrs) {
                scope.$watch('dag_run', function(){
                    if(scope.dag_run){
                        scope.render(scope.dag_run);
                    }
                });
                scope.render = function(dag_run) {
                    var tasks = dag_run.tasks;

                    var barHeight = 50;
                    var gap = barHeight + 4;
                    var sidePadding = 75;

                    var w = 800;
                    var h = barHeight * tasks.length + sidePadding;

                    var svg = d3.select(element[0])
                        .append("svg")
                        .attr("width", w)
                        .attr("height", h)
                        .attr("class", "svg");

                    var timeScale = d3.scaleTime()
                        .domain([
                            d3.min(tasks, function(d) { return new Date(d.startDate); }),
                            d3.max(tasks, function(d) { return new Date(d.finishDate); })
                        ])
                        .range([0, w]);

                    var categories = tasks.map(function(t){ return t.id; })

                    var colorScale = d3.scaleLinear()
                        .domain([0, tasks.length])
                        .range(["#00B9FA", "#F95002"])
                        .interpolate(d3.interpolateHcl);

                    makeGrid();
                    drawRects(tasks);

                    function makeGrid(){
                        var xAxis = d3.axisBottom(timeScale)
                            //.ticks(1, "s")
                            .tickSize(-h+20, 0, 0)
                            .tickFormat(d3.timeFormat("%X"));

                        var grid = svg.append('g')
                            .attr('class', 'grid')
                            .attr('transform', 'translate(0 , ' + (h - 50) + ')')
                            .call(xAxis)
                            .selectAll("text")
                                .style("text-anchor", "middle")
                                .attr("fill", "#000")
                                .attr("stroke", "none")
                                .attr("font-size", 10)
                                .attr("dy", "1em");
                    }

                    function drawRects(tasks){

                        var rectangles = svg.append('g')
                            .selectAll("rect")
                            .data(tasks)
                            .enter();

                        rectangles.append("rect")
                            .attr("rx", 3)
                            .attr("ry", 3)
                            .attr("x", function(d) {
                                console.log(new Date(d.startDate))
                                return timeScale(new Date(d.startDate));
                            })
                            .attr("y", function(d, i){ return i*gap; })
                            .attr("width", function(d){ return (timeScale(new Date(d.finishDate))-timeScale(new Date(d.startDate))); })
                            .attr("height", barHeight)
                            .attr("stroke", "none")
                            .attr("fill", function(d){
                                for (var i = 0; i < categories.length; i++){
                                    if (d.id == categories[i]){
                                        return colorScale(i);
                                    }
                                }
                            })

                        rectangles.append("text")
                            .text(function(d){
                                return d.id;
                            })
                            .attr("x", function(d){
                                return (timeScale(new Date(d.finishDate))-timeScale(new Date(d.startDate)))/2 + timeScale(new Date(d.startDate));
                            })
                            .attr("y", function(d, i){
                                return i*gap + barHeight / 2 + 4;
                            })
                            .attr("font-size", 11)
                            .attr("text-anchor", "middle")
                            .attr("text-height", barHeight)
                            .attr("fill", "#fff");
                    }

                    function getCounts(arr) {
                        var i = arr.length, // var to loop over
                        obj = {}; // obj to store results
                        while (i) obj[arr[--i]] = (obj[arr[i]] || 0) + 1; // count occurrences
                        return obj;
                    }

                    function getCount(word, arr) {
                        return getCounts(arr)[word] || 0;
                    }
                };
            }
        };
    })
    .directive('jsonText', function() {
        return {
            restrict: 'A',
            require: 'ngModel',
            link: function(scope, element, attr, ngModel) {
                ngModel.$parsers.push(function(input) {
                    try{
                        var json = JSON.parse(input);
                        ngModel.$setValidity('json', true);
                        return json;
                    }catch(e){
                        ngModel.$setValidity('json', false);
                        return undefined;
                    }
                });
                ngModel.$formatters.push(function(data) {
                    return JSON.stringify(data);
                });
            }
        };
    });
})();
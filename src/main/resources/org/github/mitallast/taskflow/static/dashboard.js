(function(){
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
    .controller('DagCreateCtrl', function($scope, $http){
        $scope.operations = [
            "dummy",
            "shell"
        ];
        $scope.dag = {
           token: '',
           tasks: []
        };
        $scope.errors = false;
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
            .then(function(response){
                console.log(response);
            });
        };
    })
    .controller('DagUpdateCtrl', function($scope, $http, $routeParams){
        $scope.operations = [
            "dummy",
            "shell"
        ];
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
        $scope.updateDag = function() {
            $http.post('/api/dag', $scope.dag)
            .then(function(response){
                $scope.dag = response.data;
            });
        };
        $http.get('/api/dag/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag = response.data;
        });
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
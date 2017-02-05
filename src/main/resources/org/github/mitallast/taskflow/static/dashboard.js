(function(){
    angular.module('dag', ['ngRoute'])
    .config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){
        $locationProvider.html5Mode(false);
        $locationProvider.hashPrefix("");
        $routeProvider
            .when('/', {
                redirectTo: '/'
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
    });
})();